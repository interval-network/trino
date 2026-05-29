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

import io.airlift.log.Logger;
import org.apache.iceberg.encryption.Ciphers;
import org.apache.iceberg.encryption.EncryptedKey;
import org.apache.iceberg.encryption.KeyManagementClient;
import org.apache.iceberg.util.ByteBuffers;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Map;

import static java.util.Objects.requireNonNull;

/**
 * Wrapper around a {@link KeyManagementClient} that handles hierarchical key unwrapping.
 *
 * In hierarchical key structures, some keys are wrapped by other keys rather than
 * directly by the KMS KEK. This client detects such keys and unwraps them using
 * AES-GCM decryption with the parent key, similar to how Iceberg unwraps manifest
 * list keys in EncryptionUtil.
 */
public class HierarchicalKeyManagementClient
        implements KeyManagementClient
{
    private static final Logger log = Logger.get(HierarchicalKeyManagementClient.class);
    private static final String KEY_TIMESTAMP = "KEY_TIMESTAMP";

    private final KeyManagementClient kmsClient;
    private final Map<String, EncryptedKey> encryptionKeys;
    private final String tableKeyId;

    public HierarchicalKeyManagementClient(
            KeyManagementClient kmsClient,
            Map<String, EncryptedKey> encryptionKeys,
            String tableKeyId)
    {
        this.kmsClient = requireNonNull(kmsClient, "kmsClient is null");
        this.encryptionKeys = requireNonNull(encryptionKeys, "encryptionKeys is null");
        this.tableKeyId = requireNonNull(tableKeyId, "tableKeyId is null");
    }

    @Override
    public ByteBuffer wrapKey(ByteBuffer key, String wrappingKeyId)
    {
        // For wrapping, always use KMS directly
        return kmsClient.wrapKey(key, wrappingKeyId);
    }

    @Override
    public ByteBuffer unwrapKey(ByteBuffer wrappedKey, String keyId)
    {
        // If keyId matches tableKeyId, this is a direct KMS unwrap
        if (keyId.equals(tableKeyId)) {
            log.debug("Unwrapping key directly with KMS KEK: %s", keyId);
            return kmsClient.unwrapKey(wrappedKey, keyId);
        }

        // Check if this keyId is in the encryption keys list
        EncryptedKey encryptedKey = encryptionKeys.get(keyId);
        if (encryptedKey == null) {
            log.warn("Key %s not found in encryption keys, attempting direct KMS unwrap", keyId);
            return kmsClient.unwrapKey(wrappedKey, keyId);
        }

        String encryptedById = encryptedKey.encryptedById();

        // If wrapped directly by tableKeyId (KMS KEK): KMS-unwrap the intermediate key,
        // then AES-GCM decrypt the DEK (wrappedKey) with the raw intermediate key.
        if (encryptedById.equals(tableKeyId)) {
            log.debug("Unwrapping intermediate key %s via KMS KEK %s, then AES-GCM decrypt DEK", keyId, tableKeyId);
            ByteBuffer rawIntermKey = kmsClient.unwrapKey(encryptedKey.encryptedKeyMetadata(), tableKeyId);
            String keyTimestamp = encryptedKey.properties().get(KEY_TIMESTAMP);
            if (keyTimestamp == null) {
                throw new IllegalStateException("Key " + keyId + " is missing KEY_TIMESTAMP property");
            }
            Ciphers.AesGcmDecryptor decryptor = new Ciphers.AesGcmDecryptor(ByteBuffers.toByteArray(rawIntermKey));
            byte[] wrappedKeyBytes = ByteBuffers.toByteArray(wrappedKey);
            byte[] aadBytes = keyTimestamp.getBytes(StandardCharsets.UTF_8);
            return ByteBuffer.wrap(decryptor.decrypt(wrappedKeyBytes, aadBytes));
        }

        // Hierarchical key: wrapped by another key, not directly by KMS
        // Need to:
        // 1. Recursively unwrap the parent key
        // 2. Use AES-GCM to decrypt this key with the parent key
        log.debug("Hierarchical key detected: key %s is wrapped by key %s", keyId, encryptedById);

        // Get the parent key's encrypted metadata
        EncryptedKey parentKey = encryptionKeys.get(encryptedById);
        if (parentKey == null) {
            throw new IllegalStateException("Parent key " + encryptedById + " not found for key " + keyId);
        }

        // Recursively unwrap the parent key
        log.debug("Recursively unwrapping parent key: %s", encryptedById);
        ByteBuffer unwrappedParentKey = unwrapKey(parentKey.encryptedKeyMetadata(), encryptedById);

        // Get the parent key's timestamp for AAD (Additional Authenticated Data)
        Map<String, String> parentProperties = parentKey.properties();
        String keyTimestamp = parentProperties.get(KEY_TIMESTAMP);
        if (keyTimestamp == null) {
            throw new IllegalStateException("Parent key " + encryptedById + " is missing KEY_TIMESTAMP property");
        }

        // Use AES-GCM to decrypt the child key with the parent key
        log.debug("Decrypting key %s using AES-GCM with parent key %s", keyId, encryptedById);
        Ciphers.AesGcmDecryptor decryptor = new Ciphers.AesGcmDecryptor(ByteBuffers.toByteArray(unwrappedParentKey));
        byte[] wrappedKeyBytes = ByteBuffers.toByteArray(wrappedKey);
        byte[] aadBytes = keyTimestamp.getBytes(StandardCharsets.UTF_8);

        byte[] unwrappedKeyBytes = decryptor.decrypt(wrappedKeyBytes, aadBytes);

        log.debug("Successfully unwrapped hierarchical key %s (wrapped by %s)", keyId, encryptedById);
        return ByteBuffer.wrap(unwrappedKeyBytes);
    }

    @Override
    public void initialize(Map<String, String> properties)
    {
        kmsClient.initialize(properties);
    }

    @Override
    public void close()
    {
        kmsClient.close();
    }
}
