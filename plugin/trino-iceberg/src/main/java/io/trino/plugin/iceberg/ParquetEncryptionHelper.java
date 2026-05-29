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
package io.trino.plugin.iceberg;

import io.airlift.log.Logger;
import io.trino.parquet.crypto.DecryptionKeyRetriever;
import io.trino.parquet.crypto.FileDecryptionProperties;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.parquet.hadoop.metadata.ColumnPath;

import java.nio.ByteBuffer;
import java.util.Optional;

/**
 * Helper class to create Parquet decryption properties from Iceberg key metadata.
 */
public final class ParquetEncryptionHelper
{
    private static final Logger log = Logger.get(ParquetEncryptionHelper.class);

    // StandardKeyMetadata Avro schema (from org.apache.iceberg.encryption.StandardKeyMetadata)
    private static final String STANDARD_KEY_METADATA_SCHEMA_JSON = "{"
            + "\"type\":\"record\","
            + "\"name\":\"StandardKeyMetadata\","
            + "\"namespace\":\"org.apache.iceberg.encryption\","
            + "\"fields\":["
            + "{\"name\":\"encryption_key\",\"type\":\"bytes\"},"
            + "{\"name\":\"aad_prefix\",\"type\":[\"null\",\"bytes\"],\"default\":null},"
            + "{\"name\":\"file_length\",\"type\":[\"null\",\"long\"],\"default\":null}"
            + "]"
            + "}";

    private static final Schema STANDARD_KEY_METADATA_SCHEMA = new Schema.Parser().parse(STANDARD_KEY_METADATA_SCHEMA_JSON);

    private ParquetEncryptionHelper() {}

    /**
     * Creates Parquet FileDecryptionProperties from Iceberg StandardKeyMetadata.
     * <p>
     * StandardKeyMetadata is Avro-encoded with schema:
     * - encryption_key: bytes (required) - unwrapped file DEK
     * - aad_prefix: bytes (optional) - AAD prefix for Parquet encryption
     * - file_length: long (optional) - file length
     *
     * @param keyMetadataBytes The serialized StandardKeyMetadata from DataFile.keyMetadata()
     * @return FileDecryptionProperties for Trino's Parquet reader, or empty if not encrypted
     */
    public static Optional<FileDecryptionProperties> createDecryptionProperties(Optional<byte[]> keyMetadataBytes)
    {
        if (keyMetadataBytes.isEmpty()) {
            return Optional.empty();
        }

        try {
            byte[] keyMetadata = keyMetadataBytes.get();

            // StandardKeyMetadata format: 1 byte version + Avro-encoded data
            // Skip the first byte (schema version) before Avro deserialization
            if (keyMetadata.length < 2) {
                throw new IllegalArgumentException("Key metadata too short: " + keyMetadata.length + " bytes");
            }

            byte schemaVersion = keyMetadata[0];
            log.debug("StandardKeyMetadata schema version: %d", schemaVersion);

            // Deserialize Avro-encoded StandardKeyMetadata (skip version byte)
            GenericDatumReader<GenericRecord> reader = new GenericDatumReader<>(STANDARD_KEY_METADATA_SCHEMA);
            BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(keyMetadata, 1, keyMetadata.length - 1, null);
            GenericRecord record = reader.read(null, decoder);

            // Extract encryption key (required field)
            ByteBuffer encryptionKeyBuffer = (ByteBuffer) record.get("encryption_key");
            byte[] fileKey = new byte[encryptionKeyBuffer.remaining()];
            encryptionKeyBuffer.duplicate().get(fileKey);

            // Extract AAD prefix (optional field)
            byte[] aadPrefix = null;
            ByteBuffer aadPrefixBuffer = (ByteBuffer) record.get("aad_prefix");
            if (aadPrefixBuffer != null) {
                aadPrefix = new byte[aadPrefixBuffer.remaining()];
                aadPrefixBuffer.duplicate().get(aadPrefix);
            }

            log.debug("Parsed StandardKeyMetadata: key length=%d bytes, AAD prefix=%s",
                    fileKey.length, aadPrefix != null ? aadPrefix.length + " bytes" : "none");

            // Create a DecryptionKeyRetriever that returns the unwrapped file key
            // StandardEncryptionManager has already unwrapped the key for us
            DecryptionKeyRetriever keyRetriever = new StandardKeyMetadataKeyRetriever(fileKey);

            // Create Trino's FileDecryptionProperties
            FileDecryptionProperties.Builder builder = FileDecryptionProperties.builder()
                    .withKeyRetriever(keyRetriever);

            if (aadPrefix != null) {
                builder.withAadPrefix(aadPrefix);
            }

            return Optional.of(builder.build());
        }
        catch (Exception e) {
            log.error(e, "Failed to create Parquet decryption properties from key metadata");
            throw new RuntimeException("Failed to create Parquet decryption properties: " + e.getMessage(), e);
        }
    }

    /**
     * Simple DecryptionKeyRetriever that returns the unwrapped file key from StandardKeyMetadata.
     * The key has already been unwrapped by Iceberg's StandardEncryptionManager.
     */
    private static class StandardKeyMetadataKeyRetriever
            implements DecryptionKeyRetriever
    {
        private final byte[] fileKey;

        public StandardKeyMetadataKeyRetriever(byte[] fileKey)
        {
            this.fileKey = fileKey;
        }

        @Override
        public Optional<byte[]> getColumnKey(ColumnPath columnPath, Optional<byte[]> keyMetadata)
        {
            // StandardEncryptionManager uses the same key for all columns
            return Optional.of(fileKey);
        }

        @Override
        public Optional<byte[]> getFooterKey(Optional<byte[]> keyMetadata)
        {
            // StandardEncryptionManager uses the same key for footer
            return Optional.of(fileKey);
        }
    }
}
