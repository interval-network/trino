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
import org.apache.iceberg.encryption.PlaintextEncryptionManager;
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
 * Extension of RESTSessionCatalog that enables PARE encryption for REST-catalog tables whose
 * metadata declares encryption. Lives in org.apache.iceberg.rest to access the package-private
 * RESTTableOperations. Mirrors the native metastore path (AbstractIcebergTableOperations), which
 * derives one EncryptionManager per table and exposes it through both io() and encryption().
 *
 * <p>Both overrides are load-bearing, and are driven from the same manager instance:
 * <ul>
 *   <li>{@code encryption()} — Trino 483's data-file read path (IcebergSplitSource) unwraps each
 *       Parquet file's DEK via {@code icebergTable.encryption().decrypt(...)}. Without this override
 *       the table inherits the plaintext default and encrypted data files are never decrypted.
 *   <li>{@code io()} — returns an EncryptingFileIO so manifest/metadata reads are decrypted, and so
 *       RESTTableOperations.io() does not re-wrap using its (null) internal keyManagementClient.
 * </ul>
 * A single manager instance backs both, keeping them consistent — important because
 * DynamicEncryptionManager mutates its delegate on upgrade().
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
        EncryptedTableIo encrypted = resolveEncryption(fileIO, current);
        return new RESTTableOperations(
                restClient,
                path,
                readHeaders,
                mutationHeaders,
                encrypted.io(),
                current,
                endpoints)
        {
            @Override
            public FileIO io()
            {
                return encrypted.io();
            }

            @Override
            public EncryptionManager encryption()
            {
                return encrypted.encryption();
            }
        };
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
        EncryptedTableIo encrypted = resolveEncryption(fileIO, current);
        return new RESTTableOperations(
                restClient,
                path,
                readHeaders,
                mutationHeaders,
                encrypted.io(),
                updateType,
                createChanges,
                current,
                endpoints)
        {
            @Override
            public FileIO io()
            {
                return encrypted.io();
            }

            @Override
            public EncryptionManager encryption()
            {
                return encrypted.encryption();
            }
        };
    }

    /**
     * Resolves the FileIO and EncryptionManager for a table. When the metadata declares encryption
     * (an encryption-keys array for Trino-written PARE, or the encryption.key-id property for
     * Spark-written PARE, whose DEKs live in each file's Avro metadata rather than in the table
     * metadata JSON), returns an EncryptingFileIO and the hierarchical manager built by the factory,
     * both backed by the same manager instance. Otherwise returns the untouched FileIO and the
     * plaintext manager (the same result as the default RESTTableOperations behavior).
     */
    EncryptedTableIo resolveEncryption(FileIO fileIO, TableMetadata metadata)
    {
        if (metadata == null) {
            return new EncryptedTableIo(fileIO, PlaintextEncryptionManager.instance());
        }

        boolean hasEncryptionKeys = metadata.encryptionKeys() != null && !metadata.encryptionKeys().isEmpty();
        boolean hasEncryptionKeyId = metadata.properties() != null
                && metadata.properties().containsKey("encryption.key-id");

        if (!hasEncryptionKeys && !hasEncryptionKeyId) {
            return new EncryptedTableIo(fileIO, PlaintextEncryptionManager.instance());
        }

        return encryptionManagerFactory.create(metadata)
                .map(manager -> {
                    log.debug("Wrapping FileIO with TrinoEncryptingFileIO for encrypted REST catalog table");
                    FileIO encryptingIo = TrinoEncryptingFileIO.wrap(new PropertyExposingFileIO(fileIO), manager, fileIO);
                    return new EncryptedTableIo(encryptingIo, manager);
                })
                .orElseGet(() -> {
                    log.warn("EncryptionManager factory returned empty for encrypted table; using plain FileIO");
                    return new EncryptedTableIo(fileIO, PlaintextEncryptionManager.instance());
                });
    }

    record EncryptedTableIo(FileIO io, EncryptionManager encryption) {}
}
