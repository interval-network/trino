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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Ticker;
import com.google.common.cache.Cache;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ExecutionError;
import com.google.common.util.concurrent.UncheckedExecutionException;
import io.airlift.units.Duration;
import io.trino.cache.EvictableCacheBuilder;
import org.apache.iceberg.rest.RESTClient;
import org.apache.iceberg.rest.responses.LoadTableResponse;

import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

import static io.trino.cache.CacheUtils.uncheckedCacheGet;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

/// Interval fork (PLE-551): a short-TTL, per-catalog cache of Iceberg REST `loadTable` responses, shared by
/// every query on this node.
///
/// Trino 483 caches a loaded table only for the life of one query (`TrinoRestCatalog.tableCache`), so every
/// query that touches a table pays a `GET /v1/{prefix}/namespaces/{ns}/tables/{table}` round trip — 63-84 ms
/// p50 and 0.5-0.6 s p95 against Gravitino under SOV-148 load. This cache sits under `RESTSessionCatalog`, at
/// the `RESTClient` boundary, and remembers the parsed [LoadTableResponse]. Caching the response rather than
/// the built `Table` keeps everything downstream per query, exactly as before: `RESTSessionCatalog` still
/// builds a fresh `RESTTableOperations`, a fresh `FileIO` from the querying session's own identity, and
/// (through `EncryptionAwareRESTSessionCatalog`) a fresh encryption manager from the cached metadata's
/// `encryption-keys` and `encryption.key-id`. Only immutable state is shared: the `TableMetadata` and the
/// table `config` map. Iceberg's own `CachingCatalog` shares far more — whole `Table` objects across threads.
/// The factory also refuses the cache with `iceberg.rest-catalog.vended-credentials-enabled`, because vended
/// storage credentials expire and a cached response would replay them past their lifetime.
///
/// **Identity.** Entries are keyed by request path and query parameters only. That is safe because the
/// cache is only built for `iceberg.rest-catalog.session=NONE` (enforced in `TrinoIcebergRestCatalogFactory`):
/// with session NONE, `TrinoRestCatalog`
/// sends a `SessionContext` with no user and only the catalog's own credential, and
/// `SharedSessionOAuth2Manager.contextualSession` refuses anything else. Every request therefore goes to the
/// REST server as the one catalog principal, and the end user's identity reaches only the `FileIO` built per
/// query. Gravitino's per-principal authorization sees the same principal whether or not this cache answers.
///
/// **What is cached.** Only a `GET` that `RESTSessionCatalog.loadTable` issues: response type
/// [LoadTableResponse], no per-call headers (no `If-None-Match`, which Iceberg's own table cache would add —
/// that cache is disabled in `IcebergRestCatalogPropertiesProvider`), via the overload that collects response
/// headers. `RESTTableOperations.refresh()` uses a different overload and always reaches the server, so a
/// commit retry after a `CommitFailedException` re-reads live metadata instead of the cached copy. Errors
/// (including "no such table") are never cached.
///
/// **Invalidation.** Any `POST` or `DELETE` this node sends (a commit, create, drop, rename, register,
/// multi-table transaction), other than a scan-metrics report, drops the whole cache both before it is
/// sent and after it returns or fails. The cache is built with [EvictableCacheBuilder], so a load that was
/// already in flight when an invalidation happened is not stored. A node-local write is therefore visible
/// to this node's next query. A write by another engine (Spark, another Trino) is not seen here until the
/// entry expires, so a reader on this node may see a snapshot up to `iceberg.rest-catalog.table-cache-ttl`
/// old. That staleness is the trade-off this cache makes.
public final class RestTableMetadataCache
{
    private final Cache<TableRequestKey, LoadTableResponse> cache;
    private final AtomicLong requests = new AtomicLong();
    private final AtomicLong hits = new AtomicLong();

    public RestTableMetadataCache(Duration ttl, long maxSize)
    {
        this(ttl, maxSize, Ticker.systemTicker());
    }

    @VisibleForTesting
    RestTableMetadataCache(Duration ttl, long maxSize, Ticker ticker)
    {
        requireNonNull(ttl, "ttl is null");
        if (ttl.roundTo(NANOSECONDS) <= 0) {
            throw new IllegalArgumentException("table cache TTL must be positive: " + ttl);
        }
        this.cache = EvictableCacheBuilder.newBuilder()
                .ticker(ticker)
                .expireAfterWrite(ttl.roundTo(NANOSECONDS), NANOSECONDS)
                .maximumSize(maxSize)
                .build();
    }

    /// Returns a client that serves `loadTable` from this cache and invalidates it on every mutation.
    public RESTClient wrap(RESTClient delegate)
    {
        return new CachingRestClient(delegate, this);
    }

    LoadTableResponse getOrLoad(String path, Map<String, String> queryParams, Supplier<LoadTableResponse> loader)
    {
        requests.incrementAndGet();
        TableRequestKey key = new TableRequestKey(path, queryParams == null ? ImmutableMap.of() : ImmutableMap.copyOf(queryParams));
        boolean[] loaded = new boolean[1];
        try {
            LoadTableResponse response = uncheckedCacheGet(cache, key, () -> {
                loaded[0] = true;
                return loader.get();
            });
            if (!loaded[0]) {
                hits.incrementAndGet();
            }
            return response;
        }
        catch (UncheckedExecutionException e) {
            // Rethrow what the REST client threw (NoSuchTableException, ForbiddenException, ...) unchanged, so
            // RESTSessionCatalog and TrinoRestCatalog map errors exactly as they do without the cache.
            if (e.getCause() instanceof RuntimeException runtimeException) {
                throw runtimeException;
            }
            throw e;
        }
        catch (ExecutionError e) {
            if (e.getCause() instanceof Error error) {
                throw error;
            }
            throw e;
        }
    }

    public void invalidateAll()
    {
        cache.invalidateAll();
    }

    @VisibleForTesting
    long size()
    {
        return cache.size();
    }

    @VisibleForTesting
    long requestCount()
    {
        return requests.get();
    }

    @VisibleForTesting
    long hitCount()
    {
        return hits.get();
    }

    private record TableRequestKey(String path, Map<String, String> queryParams)
    {
        private TableRequestKey
        {
            requireNonNull(path, "path is null");
            requireNonNull(queryParams, "queryParams is null");
        }
    }
}
