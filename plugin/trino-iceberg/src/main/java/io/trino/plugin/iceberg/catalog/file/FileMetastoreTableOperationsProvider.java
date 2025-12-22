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
package io.trino.plugin.iceberg.catalog.file;

import com.google.inject.Inject;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.plugin.iceberg.catalog.IcebergTableOperations;
import io.trino.plugin.iceberg.catalog.IcebergTableOperationsProvider;
import io.trino.plugin.iceberg.catalog.TrinoCatalog;
import io.trino.plugin.iceberg.catalog.hms.DynamicEncryptionManager;
import io.trino.plugin.iceberg.catalog.hms.TrinoHiveCatalog;
import io.trino.plugin.iceberg.fileio.ForwardingFileIoFactory;
import io.trino.spi.connector.ConnectorSession;

import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import static io.trino.plugin.iceberg.IcebergSessionProperties.isUseFileSizeFromMetadata;
import static java.util.Objects.requireNonNull;

public class FileMetastoreTableOperationsProvider
        implements IcebergTableOperationsProvider
{
    private final TrinoFileSystemFactory fileSystemFactory;
    private final ForwardingFileIoFactory fileIoFactory;
    private final io.trino.plugin.iceberg.catalog.hms.EncryptionManagerFactory encryptionManagerFactory;

    // Cache DynamicEncryptionManager instances per table to ensure all TableOperations
    // for the same table share the same instance. This allows encryption state to be
    // shared across query planning and split generation threads.
    private final ConcurrentHashMap<TableKey, DynamicEncryptionManager> encryptionManagerCache = new ConcurrentHashMap<>();

    @Inject
    public FileMetastoreTableOperationsProvider(
            TrinoFileSystemFactory fileSystemFactory,
            ForwardingFileIoFactory fileIoFactory,
            io.trino.plugin.iceberg.catalog.hms.EncryptionManagerFactory encryptionManagerFactory)
    {
        this.fileSystemFactory = requireNonNull(fileSystemFactory, "fileSystemFactory is null");
        this.fileIoFactory = requireNonNull(fileIoFactory, "fileIoFactory is null");
        this.encryptionManagerFactory = requireNonNull(encryptionManagerFactory, "encryptionManagerFactory is null");
    }

    @Override
    public IcebergTableOperations createTableOperations(
            TrinoCatalog catalog,
            ConnectorSession session,
            String database,
            String table,
            Optional<String> owner,
            Optional<String> location)
    {
        // Get or create a shared DynamicEncryptionManager for this table
        // This ensures all TableOperations instances for the same table share encryption state
        TableKey key = new TableKey(database, table);
        DynamicEncryptionManager sharedEncryptionManager = encryptionManagerCache.computeIfAbsent(
                key,
                k -> new DynamicEncryptionManager());

        return new FileMetastoreTableOperations(
                fileIoFactory.create(fileSystemFactory.create(session), isUseFileSizeFromMetadata(session)),
                ((TrinoHiveCatalog) catalog).getMetastore(),
                encryptionManagerFactory,
                session,
                database,
                table,
                owner,
                location,
                sharedEncryptionManager);
    }

    /**
     * Key for caching DynamicEncryptionManager instances per table.
     */
    private record TableKey(String database, String table)
    {
        private TableKey
        {
            requireNonNull(database, "database is null");
            requireNonNull(table, "table is null");
        }
    }
}
