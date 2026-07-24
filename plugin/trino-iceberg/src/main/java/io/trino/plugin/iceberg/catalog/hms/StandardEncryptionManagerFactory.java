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
package io.trino.plugin.iceberg.catalog.hms;

import com.google.common.annotations.VisibleForTesting;
import com.google.inject.Inject;
import io.airlift.log.Logger;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.encryption.EncryptedKey;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.encryption.KeyManagementClient;
import org.apache.iceberg.encryption.StandardEncryptionManager;
import org.apache.iceberg.gcp.GcpKeyManagementClient;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

/**
 * Factory that creates StandardEncryptionManager instances for encrypted Iceberg tables using GCP KMS.
 * The shared GcpKeyManagementClient is constructed once at injection time and reused across all
 * create() calls. Per-table HierarchicalKeyManagementClient wrappers receive the shared client via
 * NonClosingKmsWrapper to prevent accidental close propagation.
 */
public class StandardEncryptionManagerFactory
        implements EncryptionManagerFactory
{
    private static final Logger log = Logger.get(StandardEncryptionManagerFactory.class);
    private static final int DEFAULT_DATA_KEY_LENGTH = 16;

    // Null when encryption is not configured (no kms-key-uri); create() returns null in that case.
    private final GcpKeyManagementClient sharedGcpKmsClient;

    @Inject
    public StandardEncryptionManagerFactory(IcebergEncryptionConfig encryptionConfig)
    {
        requireNonNull(encryptionConfig, "encryptionConfig is null");
        String kmsKeyUri = encryptionConfig.getKmsKeyUri();
        this.sharedGcpKmsClient = (kmsKeyUri != null && !kmsKeyUri.isBlank())
                ? buildAndInitializeKmsClient(encryptionConfig)
                : null;
    }

    @VisibleForTesting
    StandardEncryptionManagerFactory(GcpKeyManagementClient sharedGcpKmsClient)
    {
        this.sharedGcpKmsClient = sharedGcpKmsClient;
    }

    @VisibleForTesting
    GcpKeyManagementClient sharedKmsClientForTesting()
    {
        return sharedGcpKmsClient;
    }

    private static GcpKeyManagementClient buildAndInitializeKmsClient(IcebergEncryptionConfig encryptionConfig)
    {
        requireNonNull(encryptionConfig, "encryptionConfig is null");

        // GcpKeyManagementClient's ByteStringShim static initializer uses DynClasses.builder()
        // which captures Thread.currentThread().getContextClassLoader(). At @Inject time (Guice
        // thread) the context class loader is the system loader, not the plugin loader, so
        // protobuf-java is invisible. Temporarily set the context loader to the plugin loader
        // so ByteStringShim.<clinit> resolves com.google.protobuf.ByteString correctly.
        ClassLoader pluginLoader = StandardEncryptionManagerFactory.class.getClassLoader();
        ClassLoader originalLoader = Thread.currentThread().getContextClassLoader();
        Thread.currentThread().setContextClassLoader(pluginLoader);
        GcpKeyManagementClient client;
        try {
            client = new GcpKeyManagementClient();
            // ByteStringShim is a static inner class whose <clinit> uses DynClasses.builder(),
            // which captures the context classloader. <clinit> runs lazily on the first
            // wrapKey/unwrapKey call — long after we've restored the context classloader. Force
            // it to initialize NOW while the plugin classloader is still the context classloader.
            try {
                Class.forName("org.apache.iceberg.gcp.GcpKeyManagementClient$ByteStringShim", true, pluginLoader);
                log.info("GcpKeyManagementClient$ByteStringShim initialized with plugin classloader");
            }
            catch (Throwable t) {
                log.warn(t,
                        "Could not eagerly initialize GcpKeyManagementClient$ByteStringShim; type=%s msg=%s",
                        t.getClass().getName(),
                        t.getMessage());
            }
        }
        finally {
            Thread.currentThread().setContextClassLoader(originalLoader);
        }
        Map<String, String> properties = new HashMap<>();
        String kmsKeyUri = encryptionConfig.getKmsKeyUri();
        if (kmsKeyUri != null && !kmsKeyUri.isEmpty()) {
            properties.put("encryption.kms.key-uri", kmsKeyUri);
        }
        client.initialize(properties);
        log.info("Initialized shared GcpKeyManagementClient (Guice connector singleton) with %d properties", properties.size());
        return client;
    }

    @Override
    public Optional<EncryptionManager> create(TableMetadata metadata)
    {
        requireNonNull(metadata, "metadata is null");

        if (sharedGcpKmsClient == null) {
            return Optional.empty();
        }

        Map<String, String> properties = metadata.properties();
        String tableKeyId = properties.get("encryption.key-id");

        List<EncryptedKey> encryptionKeys = metadata.encryptionKeys();
        boolean hasEncryptionKeys = encryptionKeys != null && !encryptionKeys.isEmpty();
        if (!hasEncryptionKeys && tableKeyId == null) {
            log.debug("Table %s is not encrypted (no encryption.key-id, no encryption-keys)", metadata.metadataFileLocation());
            return Optional.empty();
        }

        if (tableKeyId == null) {
            log.warn("Table has encryption keys but no encryption.key-id property");
            return Optional.empty();
        }

        int dataKeyLength = DEFAULT_DATA_KEY_LENGTH;
        if (properties.containsKey("encryption.data-key-length")) {
            try {
                dataKeyLength = Integer.parseInt(properties.get("encryption.data-key-length"));
                log.debug("Using data key length from table properties: %d bytes", dataKeyLength);
            }
            catch (NumberFormatException e) {
                log.warn("Invalid encryption.data-key-length property value: %s, using default: %d bytes",
                        properties.get("encryption.data-key-length"),
                        DEFAULT_DATA_KEY_LENGTH);
            }
        }

        List<EncryptedKey> keys = (encryptionKeys != null) ? encryptionKeys : List.of();
        log.debug(
                "Creating StandardEncryptionManager for table with %d encryption keys, table key ID: %s, data key length: %d bytes",
                keys.size(),
                tableKeyId,
                dataKeyLength);

        Map<String, EncryptedKey> encryptionKeysMap = new HashMap<>();
        for (EncryptedKey key : keys) {
            encryptionKeysMap.put(key.keyId(), key);
        }

        // Pass the shared singleton through NonClosingKmsWrapper into HKMC.
        // HKMC.close() propagates to its kmsClient.close(); the wrapper absorbs
        // that close so any caller of the returned StandardEncryptionManager
        // cannot shut down the shared client.
        KeyManagementClient kmsClient = new HierarchicalKeyManagementClient(
                new NonClosingKmsWrapper(sharedGcpKmsClient),
                encryptionKeysMap,
                tableKeyId);

        if (keys.isEmpty()) {
            log.debug("No encryption-keys in table metadata (Spark-written PARE); using tableKeyId-only constructor");
            return Optional.of(new StandardEncryptionManager(tableKeyId, dataKeyLength, kmsClient));
        }
        return Optional.of(new StandardEncryptionManager(keys, tableKeyId, dataKeyLength, kmsClient));
    }
}
