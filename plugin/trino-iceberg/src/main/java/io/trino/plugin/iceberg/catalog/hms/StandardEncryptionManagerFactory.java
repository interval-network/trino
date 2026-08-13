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
 * Factory that creates StandardEncryptionManager instances for encrypted Iceberg tables.
 * The shared KeyManagementClient is constructed once at injection time and reused across all
 * create() calls. Per-table HierarchicalKeyManagementClient wrappers receive the shared client via
 * NonClosingKmsWrapper to prevent accidental close propagation.
 *
 * <p>The KMS client is pluggable. When {@code encryption.kms-impl} names a
 * {@link KeyManagementClient} class other than {@link GcpKeyManagementClient} (e.g. the OpenBao
 * Transit client used by the local-kms / on-prem platform profiles), it is instantiated reflectively
 * via the plugin classloader. Absent {@code encryption.kms-impl}, the factory defaults to
 * {@link GcpKeyManagementClient}, preserving the production GCP-KMS behavior unchanged.
 *
 * <p>Note this is the fork's REST-catalog encryption path. Trino's own
 * {@code io.trino.plugin.iceberg.encryption} framework resolves a KMS via the
 * {@code iceberg.encryption.kms-type} enum (AWS/AZURE/GCP), but it is wired for metastore-style
 * catalogs only and has no OpenBao value, so it does not serve this path.
 */
public class StandardEncryptionManagerFactory
        implements EncryptionManagerFactory
{
    private static final Logger log = Logger.get(StandardEncryptionManagerFactory.class);
    private static final int DEFAULT_DATA_KEY_LENGTH = 16;

    // Null when encryption is not configured (no kms-key-uri and no kms-impl); create() returns null.
    private final KeyManagementClient sharedKmsClient;

    @Inject
    public StandardEncryptionManagerFactory(IcebergEncryptionConfig encryptionConfig)
    {
        requireNonNull(encryptionConfig, "encryptionConfig is null");
        String kmsKeyUri = encryptionConfig.getKmsKeyUri();
        String kmsImpl = encryptionConfig.getKmsImpl();
        // Either property means encryption is configured. A non-GCP KMS (OpenBao) needs no
        // kms-key-uri, so gating on kms-key-uri alone would silently disable encryption for it.
        boolean encryptionConfigured = (kmsKeyUri != null && !kmsKeyUri.isBlank())
                || (kmsImpl != null && !kmsImpl.isBlank());
        this.sharedKmsClient = encryptionConfigured
                ? buildAndInitializeKmsClient(encryptionConfig)
                : null;
    }

    @VisibleForTesting
    StandardEncryptionManagerFactory(KeyManagementClient sharedKmsClient)
    {
        this.sharedKmsClient = sharedKmsClient;
    }

    @VisibleForTesting
    KeyManagementClient sharedKmsClientForTesting()
    {
        return sharedKmsClient;
    }

    /**
     * The shared, already-initialised KMS client, or empty when encryption is not configured for this
     * catalog. Exposed so Trino's <em>native</em> encryption framework can be served from the same
     * client on the REST path instead of standing up a second one — see
     * {@code io.trino.plugin.iceberg.catalog.rest.RestNativeEncryptionManagerFactory}.
     *
     * <p>Callers must fail closed on {@link Optional#empty()} for a table that declares encryption.
     * Substituting a plaintext manager there would silently read an encrypted table as cleartext.
     */
    public Optional<KeyManagementClient> sharedKmsClient()
    {
        return Optional.ofNullable(sharedKmsClient);
    }

    private static Map<String, String> kmsClientProperties(IcebergEncryptionConfig encryptionConfig)
    {
        Map<String, String> properties = new HashMap<>();
        String kmsKeyUri = encryptionConfig.getKmsKeyUri();
        if (kmsKeyUri != null && !kmsKeyUri.isEmpty()) {
            properties.put("encryption.kms.key-uri", kmsKeyUri);
        }
        return properties;
    }

    private static KeyManagementClient buildAndInitializeKmsClient(IcebergEncryptionConfig encryptionConfig)
    {
        requireNonNull(encryptionConfig, "encryptionConfig is null");

        ClassLoader pluginLoader = StandardEncryptionManagerFactory.class.getClassLoader();
        String kmsImpl = encryptionConfig.getKmsImpl();

        // Pluggable KMS: when encryption.kms-impl names a non-GCP KeyManagementClient (e.g. the
        // OpenBao client for the local-kms / on-prem profiles), instantiate it reflectively via the
        // plugin classloader so it resolves against the iceberg plugin's classpath. The client reads
        // its own connection config from initialize() properties / environment.
        if (kmsImpl != null && !kmsImpl.isBlank()
                && !kmsImpl.equals(GcpKeyManagementClient.class.getName())) {
            try {
                Class<?> clazz = Class.forName(kmsImpl, true, pluginLoader);
                KeyManagementClient client = (KeyManagementClient) clazz.getDeclaredConstructor().newInstance();
                Map<String, String> properties = kmsClientProperties(encryptionConfig);
                client.initialize(properties);
                log.info("Initialized shared KeyManagementClient '%s' (Guice connector singleton) with %d properties",
                        kmsImpl,
                        properties.size());
                return client;
            }
            catch (ReflectiveOperationException e) {
                throw new RuntimeException("Failed to instantiate encryption.kms-impl=" + kmsImpl, e);
            }
        }

        // Default: GCP KMS.
        // GcpKeyManagementClient's ByteStringShim static initializer uses DynClasses.builder()
        // which captures Thread.currentThread().getContextClassLoader(). At @Inject time (Guice
        // thread) the context class loader is the system loader, not the plugin loader, so
        // protobuf-java is invisible. Temporarily set the context loader to the plugin loader
        // so ByteStringShim.<clinit> resolves com.google.protobuf.ByteString correctly.
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
        Map<String, String> properties = kmsClientProperties(encryptionConfig);
        client.initialize(properties);
        log.info("Initialized shared GcpKeyManagementClient (Guice connector singleton) with %d properties", properties.size());
        return client;
    }

    @Override
    public Optional<EncryptionManager> create(TableMetadata metadata)
    {
        requireNonNull(metadata, "metadata is null");

        if (sharedKmsClient == null) {
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
                new NonClosingKmsWrapper(sharedKmsClient),
                encryptionKeysMap,
                tableKeyId);

        if (keys.isEmpty()) {
            log.debug("No encryption-keys in table metadata (Spark-written PARE); using tableKeyId-only constructor");
            return Optional.of(new StandardEncryptionManager(tableKeyId, dataKeyLength, kmsClient));
        }
        return Optional.of(new StandardEncryptionManager(keys, tableKeyId, dataKeyLength, kmsClient));
    }
}
