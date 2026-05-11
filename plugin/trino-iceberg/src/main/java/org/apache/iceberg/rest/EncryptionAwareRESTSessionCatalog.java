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
package org.apache.iceberg.rest;

import io.airlift.log.Logger;
import io.trino.plugin.iceberg.catalog.hms.EncryptionManagerFactory;
import io.trino.plugin.iceberg.catalog.hms.PropertyExposingFileIO;
import org.apache.iceberg.MetadataUpdate;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.catalog.SessionCatalog;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.encryption.TrinoEncryptingFileIO;
import org.apache.iceberg.io.FileIO;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;

/**
 * Extension of RESTSessionCatalog that wraps FileIO with PARE encryption when the
 * table metadata contains encryption keys. Lives in org.apache.iceberg.rest to access
 * the package-private RESTTableOperations. Mirrors the HMS encryption pattern
 * (AbstractMetastoreTableOperations) but applies it at table-ops creation time since
 * the full TableMetadata is already available at that point.
 */
public class EncryptionAwareRESTSessionCatalog
        extends RESTSessionCatalog
{
    private static final Logger log = Logger.get(EncryptionAwareRESTSessionCatalog.class);

    private final EncryptionManagerFactory encryptionManagerFactory;

    public EncryptionAwareRESTSessionCatalog(
            Function<Map<String, String>, RESTClient> clientBuilder,
            BiFunction<SessionCatalog.SessionContext, Map<String, String>, FileIO> ioBuilder,
            EncryptionManagerFactory encryptionManagerFactory)
    {
        super(clientBuilder, ioBuilder);
        this.encryptionManagerFactory = requireNonNull(encryptionManagerFactory, "encryptionManagerFactory is null");
    }

    @Override
    protected RESTTableOperations newTableOps(
            RESTClient restClient,
            String path,
            Supplier<Map<String, String>> readHeaders,
            Supplier<Map<String, String>> mutationHeaders,
            FileIO fileIO,
            TableMetadata current,
            Set<Endpoint> endpoints)
    {
        return super.newTableOps(
                restClient,
                path,
                readHeaders,
                mutationHeaders,
                maybeWrapWithEncryption(fileIO, current),
                current,
                endpoints);
    }

    @Override
    protected RESTTableOperations newTableOps(
            RESTClient restClient,
            String path,
            Supplier<Map<String, String>> readHeaders,
            Supplier<Map<String, String>> mutationHeaders,
            FileIO fileIO,
            RESTTableOperations.UpdateType updateType,
            List<MetadataUpdate> createChanges,
            TableMetadata current,
            Set<Endpoint> endpoints)
    {
        return super.newTableOps(
                restClient,
                path,
                readHeaders,
                mutationHeaders,
                maybeWrapWithEncryption(fileIO, current),
                updateType,
                createChanges,
                current,
                endpoints);
    }

    private FileIO maybeWrapWithEncryption(FileIO fileIO, TableMetadata metadata)
    {
        if (metadata == null) {
            return fileIO;
        }

        // Check for encryption via the encryption-keys array (Trino-written PARE)
        // or via the encryption.key-id table property (Spark-written PARE, where DEKs
        // are stored in each file's avro metadata rather than in table metadata JSON).
        boolean hasEncryptionKeys = metadata.encryptionKeys() != null && !metadata.encryptionKeys().isEmpty();
        boolean hasEncryptionKeyId = metadata.properties() != null
                && metadata.properties().containsKey("encryption.key-id");

        if (!hasEncryptionKeys && !hasEncryptionKeyId) {
            return fileIO;
        }

        EncryptionManager encryptionManager = encryptionManagerFactory.create(metadata);
        if (encryptionManager == null) {
            log.warn("EncryptionManager factory returned null for encrypted table; using plain FileIO");
            return fileIO;
        }

        log.debug("Wrapping FileIO with TrinoEncryptingFileIO for encrypted REST catalog table");
        return TrinoEncryptingFileIO.wrap(new PropertyExposingFileIO(fileIO), encryptionManager, fileIO);
    }
}
