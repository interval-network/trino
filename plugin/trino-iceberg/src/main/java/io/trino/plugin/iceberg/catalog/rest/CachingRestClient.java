/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.plugin.iceberg.catalog.rest;

import org.apache.iceberg.rest.ParserContext;
import org.apache.iceberg.rest.RESTClient;
import org.apache.iceberg.rest.RESTRequest;
import org.apache.iceberg.rest.RESTResponse;
import org.apache.iceberg.rest.auth.AuthSession;
import org.apache.iceberg.rest.requests.ReportMetricsRequest;
import org.apache.iceberg.rest.responses.ErrorResponse;
import org.apache.iceberg.rest.responses.LoadTableResponse;

import java.io.IOException;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;

/// Interval fork (PLE-551): a [RESTClient] that answers `loadTable` from a [RestTableMetadataCache] and
/// drops that cache on every mutation. Every other call passes straight through. See
/// [RestTableMetadataCache] for what is cached and why it is identity-safe.
///
/// Every method that `BaseHTTPClient` implements is overridden here, so a call reaches the delegate through
/// the same overload it would without the wrapper (the `RESTClient` defaults for some overloads throw
/// "not supported").
final class CachingRestClient
        implements RESTClient
{
    private final RESTClient delegate;
    private final RestTableMetadataCache cache;

    CachingRestClient(RESTClient delegate, RestTableMetadataCache cache)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
        this.cache = requireNonNull(cache, "cache is null");
    }

    @Override
    public RESTClient withAuthSession(AuthSession session)
    {
        // Auth sessions only change the bearer token; the principal is the catalog's own under session NONE.
        // The child shares this cache so a commit sent through a table-scoped client still invalidates it.
        return new CachingRestClient(delegate.withAuthSession(session), cache);
    }

    @Override
    public void head(String path, Map<String, String> headers, Consumer<ErrorResponse> errorHandler)
    {
        delegate.head(path, headers, errorHandler);
    }

    // --- GET ---

    @Override
    public <T extends RESTResponse> T get(
            String path,
            Map<String, String> queryParams,
            Class<T> responseType,
            Map<String, String> headers,
            Consumer<ErrorResponse> errorHandler)
    {
        // RESTTableOperations.refresh() arrives here. Deliberately uncached: a refresh exists to see live state.
        return delegate.get(path, queryParams, responseType, headers, errorHandler);
    }

    @Override
    public <T extends RESTResponse> T get(
            String path,
            Map<String, String> queryParams,
            Class<T> responseType,
            Map<String, String> headers,
            Consumer<ErrorResponse> errorHandler,
            ParserContext parserContext)
    {
        return delegate.get(path, queryParams, responseType, headers, errorHandler, parserContext);
    }

    @Override
    public <T extends RESTResponse> T get(
            String path,
            Map<String, String> queryParams,
            Class<T> responseType,
            Map<String, String> headers,
            Consumer<ErrorResponse> errorHandler,
            Consumer<Map<String, String>> responseHeaders)
    {
        // RESTSessionCatalog.loadTable (loadInternal) arrives here. A conditional request (If-None-Match, sent only
        // when Iceberg's own table cache holds an ETag) must reach the server, since a 304 answer returns null.
        if (responseType != LoadTableResponse.class || (headers != null && !headers.isEmpty())) {
            return delegate.get(path, queryParams, responseType, headers, errorHandler, responseHeaders);
        }
        // On a hit no response headers are replayed. The only consumer is Iceberg's ETag table cache, which is
        // disabled, so an absent ETag changes nothing.
        LoadTableResponse response = cache.getOrLoad(
                path,
                queryParams,
                () -> (LoadTableResponse) delegate.get(path, queryParams, responseType, headers, errorHandler, responseHeaders));
        return responseType.cast(response);
    }

    // --- mutations: invalidate before sending and after the outcome, whether it succeeded or failed ---

    @Override
    public <T extends RESTResponse> T post(
            String path,
            RESTRequest body,
            Class<T> responseType,
            Map<String, String> headers,
            Consumer<ErrorResponse> errorHandler)
    {
        return mutate(body instanceof ReportMetricsRequest, () -> delegate.post(path, body, responseType, headers, errorHandler));
    }

    @Override
    public <T extends RESTResponse> T post(
            String path,
            RESTRequest body,
            Class<T> responseType,
            Map<String, String> headers,
            Consumer<ErrorResponse> errorHandler,
            Consumer<Map<String, String>> responseHeaders)
    {
        return mutate(body instanceof ReportMetricsRequest, () -> delegate.post(path, body, responseType, headers, errorHandler, responseHeaders));
    }

    @Override
    public <T extends RESTResponse> T post(
            String path,
            RESTRequest body,
            Class<T> responseType,
            Map<String, String> headers,
            Consumer<ErrorResponse> errorHandler,
            Consumer<Map<String, String>> responseHeaders,
            ParserContext parserContext)
    {
        return mutate(body instanceof ReportMetricsRequest, () -> delegate.post(path, body, responseType, headers, errorHandler, responseHeaders, parserContext));
    }

    @Override
    public <T extends RESTResponse> T delete(
            String path,
            Class<T> responseType,
            Map<String, String> headers,
            Consumer<ErrorResponse> errorHandler)
    {
        return mutate(false, () -> delegate.delete(path, responseType, headers, errorHandler));
    }

    @Override
    public <T extends RESTResponse> T delete(
            String path,
            Map<String, String> queryParams,
            Class<T> responseType,
            Map<String, String> headers,
            Consumer<ErrorResponse> errorHandler)
    {
        return mutate(false, () -> delegate.delete(path, queryParams, responseType, headers, errorHandler));
    }

    @Override
    public <T extends RESTResponse> T postForm(
            String path,
            Map<String, String> formData,
            Class<T> responseType,
            Map<String, String> headers,
            Consumer<ErrorResponse> errorHandler)
    {
        // OAuth2 token requests. They change no catalog state.
        return delegate.postForm(path, formData, responseType, headers, errorHandler);
    }

    @Override
    public void close()
            throws IOException
    {
        delegate.close();
    }

    private <T> T mutate(boolean metricsReport, Supplier<T> call)
    {
        // Identified by request type, not by path: a table named "metrics" has a commit path ending in /metrics
        if (metricsReport) {
            return call.get();
        }
        // Invalidating everything, rather than working out which table a rename or a multi-table transaction
        // touches, keeps this obviously correct. Writes from this node are rare next to reads.
        cache.invalidateAll();
        try {
            return call.get();
        }
        finally {
            // Again afterwards: a load that started before the mutation and finished after it may have read
            // the old metadata, and an ambiguous failure (timeout, 5xx) may have been applied by the server.
            cache.invalidateAll();
        }
    }
}
