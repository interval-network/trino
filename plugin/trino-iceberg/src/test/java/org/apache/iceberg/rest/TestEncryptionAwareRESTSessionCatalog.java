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

import io.trino.plugin.iceberg.catalog.hms.EncryptionManagerFactory;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.encryption.EncryptedInputFile;
import org.apache.iceberg.encryption.EncryptedOutputFile;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.encryption.PlaintextEncryptionManager;
import org.apache.iceberg.encryption.TrinoEncryptingFileIO;
import org.apache.iceberg.inmemory.InMemoryFileIO;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.rest.EncryptionAwareRESTSessionCatalog.EncryptedTableIo;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Optional;

import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;

public class TestEncryptionAwareRESTSessionCatalog
{
    private static final Schema SCHEMA = new Schema(required(1, "id", Types.LongType.get()));

    // A distinct, non-plaintext EncryptionManager, used only for identity assertions.
    private static final EncryptionManager HIERARCHICAL_MANAGER = new EncryptionManager()
    {
        @Override
        public InputFile decrypt(EncryptedInputFile encrypted)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public EncryptedOutputFile encrypt(OutputFile rawOutput)
        {
            throw new UnsupportedOperationException();
        }
    };

    @Test
    public void testEncryptedTableExposesHierarchicalManagerOnEncryption()
    {
        FileIO baseIo = new InMemoryFileIO();
        TableMetadata metadata = tableMetadata(Map.of("encryption.key-id", "key-123"));

        EncryptedTableIo resolved = catalog(_ -> Optional.of(HIERARCHICAL_MANAGER))
                .resolveEncryption(baseIo, metadata);

        // Regression guard: encryption() must be the hierarchical manager. It used to inherit the
        // plaintext default, so IcebergSplitSource's icebergTable.encryption().decrypt(...) never
        // unwrapped data-file DEKs and encrypted REST tables could not be read.
        assertThat(resolved.encryption()).isSameAs(HIERARCHICAL_MANAGER);
        // io() is an EncryptingFileIO backed by that same manager instance (single create() call).
        assertThat(resolved.io()).isInstanceOf(TrinoEncryptingFileIO.class);
    }

    @Test
    public void testPlainTableUsesPlaintextManagerAndUntouchedIo()
    {
        FileIO baseIo = new InMemoryFileIO();
        TableMetadata metadata = tableMetadata(Map.of());

        EncryptedTableIo resolved = catalog(_ -> {
            throw new AssertionError("factory must not be consulted for a plain table");
        }).resolveEncryption(baseIo, metadata);

        assertThat(resolved.encryption()).isSameAs(PlaintextEncryptionManager.instance());
        assertThat(resolved.io()).isSameAs(baseIo);
    }

    private static TableMetadata tableMetadata(Map<String, String> properties)
    {
        return TableMetadata.buildFromEmpty(3)
                .assignUUID()
                .setLocation("memory://table")
                .setCurrentSchema(SCHEMA, 1)
                .addPartitionSpec(PartitionSpec.unpartitioned())
                .addSortOrder(SortOrder.unsorted())
                .setProperties(properties)
                .build();
    }

    private static EncryptionAwareRESTSessionCatalog catalog(EncryptionManagerFactory factory)
    {
        return new EncryptionAwareRESTSessionCatalog(
                _ -> {
                    throw new UnsupportedOperationException("clientBuilder is not used in this test");
                },
                (_, _) -> {
                    throw new UnsupportedOperationException("ioBuilder is not used in this test");
                },
                factory);
    }
}
