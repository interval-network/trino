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

import static java.util.Objects.requireNonNull;

/**
 * Factory implementation that creates StandardEncryptionManager instances
 * for encrypted Iceberg tables using GCP KMS.
 */
public class StandardEncryptionManagerFactory
        implements EncryptionManagerFactory
{
    private static final Logger log = Logger.get(StandardEncryptionManagerFactory.class);
    private static final int DEFAULT_DATA_KEY_LENGTH = 16; // 128 bits = 16 bytes (default if not specified)

    private final IcebergEncryptionConfig encryptionConfig;

    @Inject
    public StandardEncryptionManagerFactory(IcebergEncryptionConfig encryptionConfig)
    {
        this.encryptionConfig = requireNonNull(encryptionConfig, "encryptionConfig is null");
    }

    @Override
    public EncryptionManager create(TableMetadata metadata)
    {
        requireNonNull(metadata, "metadata is null");

        // Extract table encryption key ID (KEK) from table properties
        // This is the master KEK (typically a KMS key URI) that wraps the table's encryption keys
        Map<String, String> properties = metadata.properties();
        String tableKeyId = properties.get("encryption.key-id");

        // Check if table has encryption: either via encryption-keys array (Trino-written PARE)
        // or via encryption.key-id property alone (Spark-written PARE, where DEKs are stored
        // in each file's avro metadata rather than in the table metadata JSON).
        List<EncryptedKey> encryptionKeys = metadata.encryptionKeys();
        boolean hasEncryptionKeys = encryptionKeys != null && !encryptionKeys.isEmpty();
        if (!hasEncryptionKeys && tableKeyId == null) {
            log.debug("Table %s is not encrypted", metadata.metadataFileLocation());
            return null;
        }

        if (tableKeyId == null) {
            log.warn("Table has encryption keys but no encryption.key-id property");
            return null;
        }

        // Read data key length from table properties
        // Spark sets this via 'encryption.data-key-length' TBLPROPERTY
        // Default to 16 bytes (128 bits) if not specified
        int dataKeyLength = DEFAULT_DATA_KEY_LENGTH;
        if (properties.containsKey("encryption.data-key-length")) {
            try {
                dataKeyLength = Integer.parseInt(properties.get("encryption.data-key-length"));
                log.info("Using data key length from table properties: %d bytes", dataKeyLength);
            }
            catch (NumberFormatException e) {
                log.warn("Invalid encryption.data-key-length property value: %s, using default: %d bytes",
                        properties.get("encryption.data-key-length"), DEFAULT_DATA_KEY_LENGTH);
            }
        }
        else {
            log.info("No encryption.data-key-length property found, using default: %d bytes", DEFAULT_DATA_KEY_LENGTH);
        }

        List<EncryptedKey> keys = (encryptionKeys != null) ? encryptionKeys : List.of();
        log.info("Creating StandardEncryptionManager for table with %d encryption keys, table key ID: %s, data key length: %d bytes",
                keys.size(), tableKeyId, dataKeyLength);

        // Create hierarchical KMS client that handles keys wrapped by other keys
        KeyManagementClient kmsClient = createKmsClient(keys, tableKeyId);

        // For Spark-written PARE tables (no encryption-keys array), use the simpler constructor.
        // DEKs are stored in each file's avro metadata and decrypted on demand via kmsClient.
        if (keys.isEmpty()) {
            log.info("No encryption-keys in table metadata (Spark-written PARE); using tableKeyId-only constructor");
            return new StandardEncryptionManager(tableKeyId, dataKeyLength, kmsClient);
        }

        return new StandardEncryptionManager(keys, tableKeyId, dataKeyLength, kmsClient);
    }

    private KeyManagementClient createKmsClient(List<EncryptedKey> encryptionKeys, String tableKeyId)
    {
        // Create GCP KMS client
        // GcpKeyManagementClient uses Application Default Credentials (ADC)
        // In GKE with Workload Identity, this automatically uses the service account
        GcpKeyManagementClient gcpKmsClient = new GcpKeyManagementClient();

        // Initialize the KMS client with properties
        // Pass the KMS key URI from configuration if available
        Map<String, String> properties = new HashMap<>();

        // Add KMS key URI from Trino configuration if present
        // This matches what Spark configures: spark.sql.catalog.iceberg.encryption.kms.key-uri
        String kmsKeyUri = encryptionConfig.getKmsKeyUri();
        if (kmsKeyUri != null && !kmsKeyUri.isEmpty()) {
            properties.put("encryption.kms.key-uri", kmsKeyUri);
            log.info("Configuring KMS client with key URI: %s", kmsKeyUri);
        }

        gcpKmsClient.initialize(properties);
        log.info("Created and initialized GcpKeyManagementClient with %d properties", properties.size());

        // Build encryption keys map for hierarchical unwrapping
        Map<String, EncryptedKey> encryptionKeysMap = new HashMap<>();
        for (EncryptedKey key : encryptionKeys) {
            encryptionKeysMap.put(key.keyId(), key);
            log.debug("Registered encryption key: id=%s, encryptedById=%s", key.keyId(), key.encryptedById());
        }

        // Wrap with hierarchical KMS client that handles keys wrapped by other keys
        // This enables recursive unwrapping: Key 2 -> Key 1 -> KMS KEK
        log.info("Wrapping GcpKeyManagementClient with HierarchicalKeyManagementClient for %d keys", encryptionKeysMap.size());
        return new HierarchicalKeyManagementClient(gcpKmsClient, encryptionKeysMap, tableKeyId);
    }
}
