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

import com.google.common.collect.ImmutableMap;
import io.airlift.testing.TestingTicker;
import io.airlift.units.Duration;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.rest.RESTClient;
import org.apache.iceberg.rest.RESTRequest;
import org.apache.iceberg.rest.RESTResponse;
import org.apache.iceberg.rest.auth.AuthSession;
import org.apache.iceberg.rest.requests.ReportMetricsRequest;
import org.apache.iceberg.rest.responses.ConfigResponse;
import org.apache.iceberg.rest.responses.ErrorResponse;
import org.apache.iceberg.rest.responses.LoadTableResponse;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

final class TestCachingRestClient
{
    private static final String TABLE_PATH = "v1/analytics/namespaces/audit/tables/events";
    private static final Map<String, String> LOAD_PARAMS = ImmutableMap.of("snapshots", "all");
    private static final Consumer<ErrorResponse> NO_ERRORS = _ -> {};
    private static final Consumer<Map<String, String>> IGNORE_HEADERS = _ -> {};

    private final TestingTicker ticker = new TestingTicker();

    @Test
    void testLoadTableIsServedFromCacheAfterFirstMiss()
    {
        FakeRestClient server = new FakeRestClient();
        RESTClient client = cache(5).wrap(server);

        LoadTableResponse first = loadTable(client);
        LoadTableResponse second = loadTable(client);

        assertThat(server.loadTableCount()).isEqualTo(1);
        assertThat(second).isSameAs(first);
    }

    @Test
    void testEntryExpiresAfterTtl()
    {
        FakeRestClient server = new FakeRestClient();
        RESTClient client = cache(5).wrap(server);

        loadTable(client);
        ticker.increment(4999, MILLISECONDS);
        loadTable(client);
        assertThat(server.loadTableCount()).isEqualTo(1);

        ticker.increment(2, MILLISECONDS);
        loadTable(client);
        assertThat(server.loadTableCount()).isEqualTo(2);
    }

    @Test
    void testDistinctTablesAndQueryParamsAreDistinctEntries()
    {
        FakeRestClient server = new FakeRestClient();
        RESTClient client = cache(5).wrap(server);

        client.get(TABLE_PATH, LOAD_PARAMS, LoadTableResponse.class, ImmutableMap.of(), NO_ERRORS, IGNORE_HEADERS);
        client.get(TABLE_PATH + "_other", LOAD_PARAMS, LoadTableResponse.class, ImmutableMap.of(), NO_ERRORS, IGNORE_HEADERS);
        client.get(TABLE_PATH, ImmutableMap.of("snapshots", "refs"), LoadTableResponse.class, ImmutableMap.of(), NO_ERRORS, IGNORE_HEADERS);

        assertThat(server.loadTableCount()).isEqualTo(3);
    }

    @Test
    void testCommitInvalidates()
    {
        FakeRestClient server = new FakeRestClient();
        RESTClient client = cache(5).wrap(server);

        loadTable(client);
        // RESTTableOperations.commit posts to the table path through a table-scoped (withAuthSession) client
        client.withAuthSession(AuthSession.EMPTY).post(TABLE_PATH, null, LoadTableResponse.class, ImmutableMap.of(), NO_ERRORS);
        loadTable(client);

        assertThat(server.loadTableCount()).isEqualTo(2);
    }

    @Test
    void testFailedCommitInvalidates()
    {
        FakeRestClient server = new FakeRestClient();
        server.failPosts = true;
        RESTClient client = cache(5).wrap(server);

        loadTable(client);
        // A failure can be ambiguous: the server may have applied the commit before the error reached us
        assertThatThrownBy(() -> client.post(TABLE_PATH, null, LoadTableResponse.class, ImmutableMap.of(), NO_ERRORS))
                .isInstanceOf(CommitFailedException.class);
        loadTable(client);

        assertThat(server.loadTableCount()).isEqualTo(2);
    }

    @Test
    void testDropAndRenameInvalidate()
    {
        FakeRestClient server = new FakeRestClient();
        RESTClient client = cache(5).wrap(server);

        loadTable(client);
        client.delete(TABLE_PATH, null, ImmutableMap.of(), NO_ERRORS);
        loadTable(client);
        // A rename posts to .../tables/rename, not the table path; the whole cache is dropped anyway
        client.post("v1/analytics/tables/rename", null, null, ImmutableMap.of(), NO_ERRORS);
        loadTable(client);

        assertThat(server.loadTableCount()).isEqualTo(3);
    }

    @Test
    void testMetricsReportDoesNotInvalidate()
    {
        FakeRestClient server = new FakeRestClient();
        RESTClient client = cache(5).wrap(server);

        loadTable(client);
        client.post(TABLE_PATH + "/metrics", ReportMetricsRequest.unknown(), null, ImmutableMap.of(), NO_ERRORS);
        loadTable(client);

        assertThat(server.loadTableCount()).isEqualTo(1);
    }

    @Test
    void testCommitToTableNamedMetricsInvalidates()
    {
        FakeRestClient server = new FakeRestClient();
        RESTClient client = cache(5).wrap(server);
        String metricsTablePath = "v1/analytics/namespaces/audit/tables/metrics";

        client.get(metricsTablePath, LOAD_PARAMS, LoadTableResponse.class, ImmutableMap.of(), NO_ERRORS, IGNORE_HEADERS);
        // A commit to a table literally named "metrics" ends in /metrics too; only the request type tells them apart
        client.post(metricsTablePath, null, LoadTableResponse.class, ImmutableMap.of(), NO_ERRORS);
        client.get(metricsTablePath, LOAD_PARAMS, LoadTableResponse.class, ImmutableMap.of(), NO_ERRORS, IGNORE_HEADERS);

        assertThat(server.loadTableCount()).isEqualTo(2);
    }

    @Test
    void testSizeIsBounded()
    {
        FakeRestClient server = new FakeRestClient();
        RestTableMetadataCache cache = new RestTableMetadataCache(new Duration(5, SECONDS), 2, ticker);
        RESTClient client = cache.wrap(server);

        for (int i = 0; i < 10; i++) {
            client.get(TABLE_PATH + i, LOAD_PARAMS, LoadTableResponse.class, ImmutableMap.of(), NO_ERRORS, IGNORE_HEADERS);
        }

        assertThat(cache.size()).isLessThanOrEqualTo(2);
    }

    @Test
    void testRefreshAlwaysReachesServer()
    {
        FakeRestClient server = new FakeRestClient();
        RESTClient client = cache(5).wrap(server);

        loadTable(client);
        // RESTTableOperations.refresh(): the overload without a response-header consumer
        client.get(TABLE_PATH, LoadTableResponse.class, ImmutableMap::of, NO_ERRORS);
        client.get(TABLE_PATH, LoadTableResponse.class, ImmutableMap::of, NO_ERRORS);

        assertThat(server.loadTableCount()).isEqualTo(3);
    }

    @Test
    void testConditionalRequestAndOtherResponsesAreNotCached()
    {
        FakeRestClient server = new FakeRestClient();
        RESTClient client = cache(5).wrap(server);

        client.get(TABLE_PATH, LOAD_PARAMS, LoadTableResponse.class, ImmutableMap.of("If-None-Match", "etag"), NO_ERRORS, IGNORE_HEADERS);
        client.get(TABLE_PATH, LOAD_PARAMS, LoadTableResponse.class, ImmutableMap.of("If-None-Match", "etag"), NO_ERRORS, IGNORE_HEADERS);
        client.get("v1/config", ImmutableMap.of(), ConfigResponse.class, ImmutableMap.of(), NO_ERRORS, IGNORE_HEADERS);
        client.get("v1/config", ImmutableMap.of(), ConfigResponse.class, ImmutableMap.of(), NO_ERRORS, IGNORE_HEADERS);

        assertThat(server.loadTableCount()).isEqualTo(2);
        assertThat(server.otherGetCount()).isEqualTo(2);
    }

    @Test
    void testErrorsAreNotCachedAndPropagateUnchanged()
    {
        FakeRestClient server = new FakeRestClient();
        server.missingTable = true;
        RESTClient client = cache(5).wrap(server);

        assertThatThrownBy(() -> loadTable(client)).isInstanceOf(NoSuchTableException.class);
        assertThatThrownBy(() -> loadTable(client)).isInstanceOf(NoSuchTableException.class);
        server.missingTable = false;
        loadTable(client);

        assertThat(server.loadTableCount()).isEqualTo(3);
    }

    @Test
    void testDelegatesEveryOtherCall()
            throws Exception
    {
        FakeRestClient server = new FakeRestClient();
        RESTClient client = cache(5).wrap(server);

        client.head(TABLE_PATH, ImmutableMap.of(), NO_ERRORS);
        client.postForm("v1/oauth/tokens", ImmutableMap.of(), null, ImmutableMap.of(), NO_ERRORS);
        client.close();

        assertThat(server.calls).containsExactly("HEAD", "POST_FORM", "CLOSE");
    }

    private RestTableMetadataCache cache(int ttlSeconds)
    {
        return new RestTableMetadataCache(new Duration(ttlSeconds, SECONDS), 100, ticker);
    }

    private static LoadTableResponse loadTable(RESTClient client)
    {
        // RESTSessionCatalog.loadInternal's call shape
        return client.get(TABLE_PATH, LOAD_PARAMS, LoadTableResponse.class, ImmutableMap.of(), NO_ERRORS, IGNORE_HEADERS);
    }

    private static final class FakeRestClient
            implements RESTClient
    {
        private final AtomicInteger loadTables = new AtomicInteger();
        private final AtomicInteger otherGets = new AtomicInteger();
        private final List<String> calls = new ArrayList<>();
        private volatile boolean failPosts;
        private volatile boolean missingTable;

        int loadTableCount()
        {
            return loadTables.get();
        }

        int otherGetCount()
        {
            return otherGets.get();
        }

        @Override
        public RESTClient withAuthSession(AuthSession session)
        {
            return this;
        }

        @Override
        public void head(String path, Map<String, String> headers, Consumer<ErrorResponse> errorHandler)
        {
            calls.add("HEAD");
        }

        @Override
        public <T extends RESTResponse> T delete(String path, Class<T> responseType, Map<String, String> headers, Consumer<ErrorResponse> errorHandler)
        {
            calls.add("DELETE");
            return null;
        }

        @Override
        public <T extends RESTResponse> T get(String path, Map<String, String> queryParams, Class<T> responseType, Map<String, String> headers, Consumer<ErrorResponse> errorHandler)
        {
            return respond(responseType);
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
            return respond(responseType);
        }

        private <T extends RESTResponse> T respond(Class<T> responseType)
        {
            if (responseType != LoadTableResponse.class) {
                otherGets.incrementAndGet();
                return null;
            }
            loadTables.incrementAndGet();
            if (missingTable) {
                throw new NoSuchTableException("Table does not exist");
            }
            return responseType.cast(LoadTableResponse.builder().withTableMetadata(newMetadata()).build());
        }

        @Override
        public <T extends RESTResponse> T post(String path, RESTRequest body, Class<T> responseType, Map<String, String> headers, Consumer<ErrorResponse> errorHandler)
        {
            calls.add("POST");
            if (failPosts) {
                throw new CommitFailedException("Requirement failed: current snapshot changed");
            }
            return null;
        }

        @Override
        public <T extends RESTResponse> T postForm(String path, Map<String, String> formData, Class<T> responseType, Map<String, String> headers, Consumer<ErrorResponse> errorHandler)
        {
            calls.add("POST_FORM");
            return null;
        }

        @Override
        public void close()
        {
            calls.add("CLOSE");
        }

        private static TableMetadata newMetadata()
        {
            Schema schema = new Schema(Types.NestedField.required(1, "id", Types.LongType.get()));
            return TableMetadata.newTableMetadata(schema, PartitionSpec.unpartitioned(), "file:/tmp/events", ImmutableMap.of());
        }
    }
}
