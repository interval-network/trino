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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.plugin.iceberg.catalog.hms.TestablePluggableKmsClient;
import io.trino.plugin.iceberg.encryption.DefaultEncryptionManagerFactory;
import io.trino.spi.TrinoException;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.encryption.KeyManagementClient;
import org.apache.iceberg.encryption.PlaintextEncryptionManager;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The behaviour under test is fail-closed encryption on the REST catalog's {@code $files} path.
 *
 * <p>{@code encryptedTableIsReadableWhenForkKmsIsConfigured} is the regression test for the live
 * incident: it reproduces the exact inputs {@code IcebergPageSourceProvider}'s {@code FilesTableSplit}
 * branch passes, and asserts against {@link DefaultEncryptionManagerFactory} that the shipped 483
 * behaviour really is a hard failure — so the test would have caught this before the cutover.
 */
public class TestRestNativeEncryptionManagerFactory
{
    private static final String KEY_ID = "gcp-kms://projects/p/locations/l/keyRings/k/cryptoKeys/c";

    /**
     * Exactly what IcebergPageSourceProvider passes for a FilesTableSplit: only the table key id.
     */
    private static Map<String, String> filesTableSplitProperties(String keyId)
    {
        return ImmutableMap.of("encryption.key-id", keyId);
    }

    @Test
    public void unencryptedTableReturnsPlaintextManager()
    {
        RestNativeEncryptionManagerFactory factory =
                new RestNativeEncryptionManagerFactory(Optional.of(new TestablePluggableKmsClient()));

        EncryptionManager manager = factory.create(ImmutableList.of(), ImmutableMap.of());

        assertThat(manager)
                .as("a table declaring no encryption.key-id must still read as plaintext")
                .isInstanceOf(PlaintextEncryptionManager.class);
    }

    @Test
    public void encryptedTableIsReadableWhenForkKmsIsConfigured()
    {
        // The shipped 483 behaviour: DefaultEncryptionManagerFactory with no iceberg.encryption.kms-type.
        // This is the live bug -- $files/$partitions on every encrypted table.
        assertThatThrownBy(() -> new DefaultEncryptionManagerFactory(Optional.<KeyManagementClient>empty())
                .create(ImmutableList.of(), filesTableSplitProperties(KEY_ID)))
                .isInstanceOf(TrinoException.class)
                .hasMessageContaining("key management client is not configured");

        // The fix: same inputs, served from the fork's already-initialised client.
        RestNativeEncryptionManagerFactory factory =
                new RestNativeEncryptionManagerFactory(Optional.of(new TestablePluggableKmsClient()));

        EncryptionManager manager = factory.create(ImmutableList.of(), filesTableSplitProperties(KEY_ID));

        assertThat(manager)
                .as("an encrypted table must get a real encryption manager, not plaintext")
                .isNotNull()
                .isNotInstanceOf(PlaintextEncryptionManager.class);
    }

    @Test
    public void encryptedTableWithoutKmsClientFailsClosed()
    {
        RestNativeEncryptionManagerFactory factory =
                new RestNativeEncryptionManagerFactory(Optional.empty());

        assertThatThrownBy(() -> factory.create(ImmutableList.of(), filesTableSplitProperties(KEY_ID)))
                .as("an encrypted table with no KMS client must FAIL, never silently read as plaintext")
                .isInstanceOf(TrinoException.class)
                .hasMessageContaining("Refusing to read an encrypted table as plaintext");
    }

    @Test
    public void blankKeyIdFailsClosed()
    {
        RestNativeEncryptionManagerFactory factory =
                new RestNativeEncryptionManagerFactory(Optional.of(new TestablePluggableKmsClient()));

        assertThatThrownBy(() -> factory.create(ImmutableList.of(), filesTableSplitProperties("   ")))
                .as("a present-but-blank encryption.key-id is malformed, not 'unencrypted'")
                .isInstanceOf(TrinoException.class)
                .hasMessageContaining("present but blank");
    }
}
