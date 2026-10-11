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
import io.airlift.http.server.HttpConfig;
import io.airlift.http.server.HttpServerConfig;
import io.airlift.http.server.HttpServerInfo;
import io.airlift.http.server.ServerFeature;
import io.airlift.http.server.testing.TestingHttpServer;
import io.airlift.node.NodeInfo;
import io.trino.plugin.iceberg.IcebergQueryRunner;
import io.trino.testing.DistributedQueryRunner;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.rest.HTTPRequest;
import org.apache.iceberg.rest.RESTCatalogAdapter;
import org.apache.iceberg.rest.RESTCatalogServlet;
import org.apache.iceberg.rest.RESTResponse;
import org.apache.iceberg.rest.responses.ErrorResponse;
import org.apache.iceberg.rest.responses.LoadTableResponse;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static io.trino.plugin.iceberg.catalog.rest.RestCatalogTestUtils.backendCatalog;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// PLE-551: the cross-query loadTable cache, end to end through real queries against an in-process REST server.
final class TestRestTableMetadataCacheQueries
{
    @Test
    void testDisabledByDefault(@TempDir Path warehouseLocation)
            throws Exception
    {
        CountingRestCatalogAdapter adapter = new CountingRestCatalogAdapter(backendCatalog(warehouseLocation));
        TestingHttpServer server = startServer(adapter);
        try (DistributedQueryRunner queryRunner = queryRunner(server, ImmutableMap.of())) {
            createTable(queryRunner, "iceberg");

            int before = adapter.loadTableCount();
            queryRunner.execute("SELECT count(*) FROM iceberg.test_schema.test_table");
            int afterFirst = adapter.loadTableCount();
            queryRunner.execute("SELECT count(*) FROM iceberg.test_schema.test_table");
            int afterSecond = adapter.loadTableCount();

            // Unchanged upstream behaviour: every query loads the table again
            assertThat(afterFirst).isGreaterThan(before);
            assertThat(afterSecond).isGreaterThan(afterFirst);
        }
        finally {
            server.stop();
        }
    }

    @Test
    void testRepeatedReadsSkipLoadTableAndOwnWritesInvalidate(@TempDir Path warehouseLocation)
            throws Exception
    {
        CountingRestCatalogAdapter adapter = new CountingRestCatalogAdapter(backendCatalog(warehouseLocation));
        TestingHttpServer server = startServer(adapter);
        try (DistributedQueryRunner queryRunner = queryRunner(server, ImmutableMap.of("iceberg.rest-catalog.table-cache-ttl", "1h"))) {
            createTable(queryRunner, "iceberg");

            assertThat(count(queryRunner, "iceberg")).isEqualTo(2);
            int warmed = adapter.loadTableCount();
            assertThat(count(queryRunner, "iceberg")).isEqualTo(2);
            assertThat(count(queryRunner, "iceberg")).isEqualTo(2);
            assertThat(adapter.loadTableCount()).isEqualTo(warmed);

            // This node's own commit drops the cache, so its next read sees the new snapshot immediately
            queryRunner.execute("INSERT INTO iceberg.test_schema.test_table VALUES (3, 'carol')");
            assertThat(count(queryRunner, "iceberg")).isEqualTo(3);

            queryRunner.execute("DELETE FROM iceberg.test_schema.test_table WHERE id = 1");
            assertThat(count(queryRunner, "iceberg")).isEqualTo(2);

            queryRunner.execute("DROP TABLE iceberg.test_schema.test_table");
            assertThat(queryRunner.execute("SHOW TABLES FROM iceberg.test_schema").getRowCount()).isZero();
        }
        finally {
            server.stop();
        }
    }

    @Test
    void testAnotherWriterIsStaleUntilVersionedReadReloads(@TempDir Path warehouseLocation)
            throws Exception
    {
        CountingRestCatalogAdapter adapter = new CountingRestCatalogAdapter(backendCatalog(warehouseLocation));
        TestingHttpServer server = startServer(adapter);
        try (DistributedQueryRunner queryRunner = queryRunner(server, ImmutableMap.of("iceberg.rest-catalog.table-cache-ttl", "1h"))) {
            // A second catalog over the same REST server, without the cache, stands in for another engine (Spark)
            queryRunner.createCatalog("iceberg_direct", "iceberg", catalogProperties(server, ImmutableMap.of()));
            createTable(queryRunner, "iceberg");
            assertThat(count(queryRunner, "iceberg")).isEqualTo(2);

            queryRunner.execute("INSERT INTO iceberg_direct.test_schema.test_table VALUES (3, 'carol')");
            long newSnapshotId = (long) queryRunner.execute(
                            "SELECT snapshot_id FROM iceberg_direct.test_schema.\"test_table$snapshots\" ORDER BY committed_at DESC LIMIT 1")
                    .getOnlyValue();

            // The accepted trade-off: the cached catalog still reads the snapshot it loaded, up to the TTL
            assertThat(count(queryRunner, "iceberg")).isEqualTo(2);

            // A read pinned to a snapshot the cached metadata lacks reloads instead of failing as unknown
            assertThat(queryRunner.execute("SELECT count(*) FROM iceberg.test_schema.test_table FOR VERSION AS OF " + newSnapshotId)
                    .getOnlyValue()).isEqualTo(3L);
            // and that reload refreshed the cache for everyone
            assertThat(count(queryRunner, "iceberg")).isEqualTo(3);
        }
        finally {
            server.stop();
        }
    }

    @Test
    void testTagCreatedByAnotherWriterReloads(@TempDir Path warehouseLocation)
            throws Exception
    {
        Catalog backend = backendCatalog(warehouseLocation);
        CountingRestCatalogAdapter adapter = new CountingRestCatalogAdapter(backend);
        TestingHttpServer server = startServer(adapter);
        try (DistributedQueryRunner queryRunner = queryRunner(server, ImmutableMap.of("iceberg.rest-catalog.table-cache-ttl", "1h"))) {
            createTable(queryRunner, "iceberg");
            assertThat(count(queryRunner, "iceberg")).isEqualTo(2);

            // Another writer tags the current snapshot after this node cached the table; a tag is a ref, which a
            // FOR VERSION AS OF '<name>' resolves through the same TARGET_ID path as a snapshot id
            Table table = backend.loadTable(TableIdentifier.of("test_schema", "test_table"));
            table.manageSnapshots().createTag("external_tag", table.currentSnapshot().snapshotId()).commit();

            assertThat(queryRunner.execute("SELECT count(*) FROM iceberg.test_schema.test_table FOR VERSION AS OF 'external_tag'")
                    .getOnlyValue()).isEqualTo(2L);
        }
        finally {
            server.stop();
        }
    }

    @Test
    void testCachesUnderOAuth2(@TempDir Path warehouseLocation)
            throws Exception
    {
        // The production target authenticates with OAuth2. Its bearer token is attached inside HTTPClient, not as
        // a per-call header, so the cache must still engage
        CountingRestCatalogAdapter adapter = new CountingRestCatalogAdapter(backendCatalog(warehouseLocation));
        TestingHttpServer server = startServer(adapter);
        try (DistributedQueryRunner queryRunner = queryRunner(server, ImmutableMap.of(
                "iceberg.rest-catalog.table-cache-ttl", "1h",
                "iceberg.rest-catalog.security", "OAUTH2",
                "iceberg.rest-catalog.oauth2.credential", "client_id:client_secret",
                "iceberg.rest-catalog.session", "NONE"))) {
            createTable(queryRunner, "iceberg");
            assertThat(count(queryRunner, "iceberg")).isEqualTo(2);
            int warmed = adapter.loadTableCount();
            assertThat(count(queryRunner, "iceberg")).isEqualTo(2);
            assertThat(adapter.loadTableCount()).isEqualTo(warmed);
        }
        finally {
            server.stop();
        }
    }

    @Test
    void testUnknownSnapshotStillFailsWithCacheEnabled(@TempDir Path warehouseLocation)
            throws Exception
    {
        CountingRestCatalogAdapter adapter = new CountingRestCatalogAdapter(backendCatalog(warehouseLocation));
        TestingHttpServer server = startServer(adapter);
        try (DistributedQueryRunner queryRunner = queryRunner(server, ImmutableMap.of("iceberg.rest-catalog.table-cache-ttl", "1h"))) {
            createTable(queryRunner, "iceberg");
            assertThatThrownBy(() -> queryRunner.execute("SELECT count(*) FROM iceberg.test_schema.test_table FOR VERSION AS OF 9876543210123"))
                    .hasMessageContaining("Iceberg snapshot ID does not exists: 9876543210123");
        }
        finally {
            server.stop();
        }
    }

    @Test
    void testRejectsUnsafeCombinations(@TempDir Path warehouseLocation)
            throws Exception
    {
        // The cache is keyed by table only: identity-safe for session NONE (one catalog principal), unsafe for
        // USER (Gravitino authorizes per principal), so a USER catalog with the cache must not start
        CountingRestCatalogAdapter adapter = new CountingRestCatalogAdapter(backendCatalog(warehouseLocation));
        TestingHttpServer server = startServer(adapter);
        try (DistributedQueryRunner queryRunner = queryRunner(server, ImmutableMap.of())) {
            assertThatThrownBy(() -> queryRunner.createCatalog("iceberg_user", "iceberg", catalogProperties(server, ImmutableMap.of(
                    "iceberg.rest-catalog.session", "USER",
                    "iceberg.rest-catalog.table-cache-ttl", "5s"))))
                    .hasStackTraceContaining("iceberg.rest-catalog.table-cache-ttl requires iceberg.rest-catalog.session=NONE");
            // Vended storage credentials expire; a cached response would replay them past their lifetime
            assertThatThrownBy(() -> queryRunner.createCatalog("iceberg_vended", "iceberg", catalogProperties(server, ImmutableMap.of(
                    "iceberg.rest-catalog.vended-credentials-enabled", "true",
                    "iceberg.rest-catalog.table-cache-ttl", "5s"))))
                    .hasStackTraceContaining("iceberg.rest-catalog.table-cache-ttl cannot be combined with iceberg.rest-catalog.vended-credentials-enabled");
        }
        finally {
            server.stop();
        }
    }

    private static void createTable(DistributedQueryRunner queryRunner, String catalog)
    {
        queryRunner.execute("CREATE SCHEMA " + catalog + ".test_schema");
        queryRunner.execute("CREATE TABLE " + catalog + ".test_schema.test_table (id INTEGER, name VARCHAR)");
        queryRunner.execute("INSERT INTO " + catalog + ".test_schema.test_table VALUES (1, 'alice'), (2, 'bob')");
    }

    private static long count(DistributedQueryRunner queryRunner, String catalog)
    {
        return (long) queryRunner.execute("SELECT count(*) FROM " + catalog + ".test_schema.test_table").getOnlyValue();
    }

    private static DistributedQueryRunner queryRunner(TestingHttpServer server, Map<String, String> extraProperties)
            throws Exception
    {
        return IcebergQueryRunner.builder()
                .setIcebergProperties(catalogProperties(server, extraProperties))
                .disableSchemaInitializer()
                .build();
    }

    private static Map<String, String> catalogProperties(TestingHttpServer server, Map<String, String> extraProperties)
    {
        return ImmutableMap.<String, String>builder()
                .put("iceberg.catalog.type", "rest")
                .put("iceberg.rest-catalog.uri", server.getBaseUrl().toString())
                .put("fs.hadoop.enabled", "true")
                .putAll(extraProperties)
                .buildOrThrow();
    }

    private static TestingHttpServer startServer(RESTCatalogAdapter adapter)
            throws Exception
    {
        NodeInfo nodeInfo = new NodeInfo("test");
        HttpServerConfig config = new HttpServerConfig()
                .setHttpEnabled(true);
        HttpServerInfo httpServerInfo = new HttpServerInfo(config, Optional.of(new HttpConfig().setHttpPort(0)), Optional.empty(), nodeInfo);
        TestingHttpServer server = new TestingHttpServer("rest-catalog", httpServerInfo, nodeInfo, config, new RESTCatalogServlet(adapter), ServerFeature.builder()
                .withLegacyUriCompliance(true)
                .build());
        server.start();
        return server;
    }

    private static final class CountingRestCatalogAdapter
            extends RESTCatalogAdapter
    {
        private final AtomicInteger loadTables = new AtomicInteger();

        CountingRestCatalogAdapter(Catalog delegate)
        {
            super(delegate);
        }

        @Override
        protected <T extends RESTResponse> T execute(
                HTTPRequest request,
                Class<T> responseType,
                Consumer<ErrorResponse> errorHandler,
                Consumer<Map<String, String>> responseHeaders)
        {
            if (request.method() == HTTPRequest.HTTPMethod.GET && responseType == LoadTableResponse.class) {
                loadTables.incrementAndGet();
            }
            return super.execute(request, responseType, errorHandler, responseHeaders);
        }

        int loadTableCount()
        {
            return loadTables.get();
        }
    }
}
