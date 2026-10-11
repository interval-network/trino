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
import io.trino.filesystem.Location;
import io.trino.testing.QueryFailedException;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Map;

import static io.trino.testing.TestingNames.randomNameSuffix;
import static org.assertj.core.api.Assertions.assertThat;

/// PLE-551: the whole REST smoke suite with the cross-query loadTable cache on and a TTL far longer than the
/// suite, so any DDL or DML path that fails to invalidate it reads stale metadata and fails here.
final class TestIcebergTrinoRestCatalogTableCacheConnectorSmokeTest
        extends TestIcebergTrinoRestCatalogConnectorSmokeTest
{
    @Override
    protected Map<String, String> additionalIcebergProperties()
    {
        return ImmutableMap.of("iceberg.rest-catalog.table-cache-ttl", "1h");
    }

    // Without the cache, DROP fails during analysis: loadTable reaches the REST server, which cannot read the
    // metadata file this test deleted behind the catalog's back, and the base test asserts exactly that failure,
    // unconditionally. With the cache, that loadTable is answered from the entry an earlier query in this test
    // left, the server is never asked for the deleted file, and the drop goes through: the catalog row
    // disappears, but the table location does not, because the server-side purge found no readable metadata to
    // delete data from, and the REST purge path does not delete the directory. A file deleted out of band is
    // invisible to this node, so no cache invalidation can cover it within the TTL. Whether the entry survived
    // depends on the suite's concurrent tests, whose writes drop the whole cache, so accept both outcomes.
    @Test
    @Override
    public void testDropTableWithMissingMetadataFile()
    {
        String tableName = "test_drop_table_with_missing_metadata_file_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " AS SELECT 1 x, 'INDIA' y", 1);

        Location metadataLocation = Location.of(getMetadataLocation(tableName));
        Location tableLocation = Location.of(getTableLocation(tableName));

        try {
            // Delete current metadata file
            fileSystem.deleteFile(metadataLocation);
            assertThat(fileSystem.newInputFile(metadataLocation).exists())
                    .describedAs("Current metadata file should not exist")
                    .isFalse();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        // try to drop table
        try {
            assertUpdate("DROP TABLE " + tableName);
        }
        catch (QueryFailedException e) {
            // The cache entry was flushed (e.g. a concurrent test's write invalidated the whole cache), so
            // loadTable reached the server and failed on the deleted file, as it always does without the cache.
            assertThat(e).hasMessageMatching("Failed to load table: (.*)");
            return;
        }
        // The drop went through on the cached metadata: the table is gone from the catalog, while the location
        // keeps its files, because the server-side purge found no readable metadata to delete data from. That
        // leftover is the price of having answered loadTable from the cache instead of the server.
        assertThat(getQueryRunner().tableExists(getSession(), tableName)).isFalse();
        try {
            assertThat(fileSystem.listFiles(tableLocation).hasNext())
                    .describedAs("Table location should still exist: the purge found no readable metadata")
                    .isTrue();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    // Without the cache this DROP fails, because the REST server cannot read the metadata file the test deleted
    // behind its back. With the cache, DROP's loadTable may be answered from the entry an earlier query left and
    // then the drop goes through, since the server is never asked for the deleted file. Whether the entry is still
    // there depends on the suite's concurrent tests, whose writes drop the whole cache, so accept either outcome.
    @Test
    @Override
    public void testDropTableWithNonExistentTableLocation()
    {
        String tableName = "test_drop_table_with_non_existent_table_location_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " AS SELECT 1 x, 'INDIA' y", 1);
        assertUpdate("INSERT INTO " + tableName + " VALUES (2, 'POLAND')", 1);
        assertQuery("SELECT count(*) FROM " + tableName, "VALUES 2");

        Location tableLocation = Location.of(getTableLocation(tableName));
        try {
            fileSystem.deleteDirectory(tableLocation);
            assertThat(fileSystem.listFiles(tableLocation).hasNext()).isFalse();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        try {
            assertUpdate("DROP TABLE " + tableName);
            assertThat(getQueryRunner().tableExists(getSession(), tableName)).isFalse();
        }
        catch (QueryFailedException e) {
            assertThat(e).hasMessageMatching("Failed to load table: (.*)");
        }
    }
}
