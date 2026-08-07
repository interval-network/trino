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

import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestStandardEncryptionManagerFactory
{
    private static final Schema TEST_SCHEMA =
            new Schema(Types.NestedField.required(1, "id", Types.LongType.get()));

    @Test
    public void productionConstructorInitializesSharedClientExactlyOnce()
    {
        IcebergEncryptionConfig config = new IcebergEncryptionConfig()
                .setKmsKeyUri("gcp-kms://projects/p/locations/l/keyRings/k/cryptoKeys/c");

        StandardEncryptionManagerFactory factory =
                new StandardEncryptionManagerFactory(config);

        assertThat(factory.sharedKmsClientForTesting())
                .as("factory must hold a non-null shared KMS client after construction")
                .isNotNull();
    }

    @Test
    public void kmsImplSelectsThePluggableClientInsteadOfGcp()
    {
        IcebergEncryptionConfig config = new IcebergEncryptionConfig()
                .setKmsImpl(TestablePluggableKmsClient.class.getName())
                .setKmsKeyUri("openbao://iceberg-pare");

        StandardEncryptionManagerFactory factory =
                new StandardEncryptionManagerFactory(config);

        assertThat(factory.sharedKmsClientForTesting())
                .as("encryption.kms-impl must be honored, not silently ignored in favour of GCP")
                .isInstanceOf(TestablePluggableKmsClient.class);

        TestablePluggableKmsClient client = (TestablePluggableKmsClient) factory.sharedKmsClientForTesting();
        assertThat(client.initializeCalls())
                .as("the shared client must be initialized exactly once, at construction")
                .isEqualTo(1);
        assertThat(client.initializeProperties())
                .as("the factory must pass kms.key-uri through to the pluggable client")
                .containsEntry("encryption.kms.key-uri", "openbao://iceberg-pare");
    }

    @Test
    public void kmsImplWithoutKeyUriStillConfiguresEncryption()
    {
        // The on-prem shape: a non-GCP KMS needs no kms-key-uri. Gating "is encryption configured?"
        // on kms-key-uri alone left the shared client null, so create() returned empty and tables
        // silently read as plaintext instead of failing.
        IcebergEncryptionConfig config = new IcebergEncryptionConfig()
                .setKmsImpl(TestablePluggableKmsClient.class.getName());

        StandardEncryptionManagerFactory factory =
                new StandardEncryptionManagerFactory(config);

        assertThat(factory.sharedKmsClientForTesting())
                .as("kms-impl alone must count as encryption being configured")
                .isInstanceOf(TestablePluggableKmsClient.class);
    }

    @Test
    public void unknownKmsImplFailsLoudly()
    {
        IcebergEncryptionConfig config = new IcebergEncryptionConfig()
                .setKmsImpl("xyz.interval.does.NotExist");

        assertThatThrownBy(() -> new StandardEncryptionManagerFactory(config))
                .as("a misconfigured kms-impl must fail at construction, not degrade to plaintext")
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("xyz.interval.does.NotExist");
    }

    @Test
    public void testConstructorStoresExactSharedClientPassedIn()
    {
        TestableGcpKmsClient sharedClient = new TestableGcpKmsClient();

        StandardEncryptionManagerFactory factory =
                new StandardEncryptionManagerFactory(sharedClient);

        assertThat(factory.sharedKmsClientForTesting())
                .as("factory must retain the passed-in shared client by identity")
                .isSameAs(sharedClient);
        assertThat(sharedClient.initializeCalls())
                .as("test constructor must NOT invoke initialize() (production ctor's job)")
                .isZero();
    }

    @Test
    public void multipleCreateCallsDoNotInvokeInitializeOrConstructNewClients()
    {
        TestableGcpKmsClient sharedClient = new TestableGcpKmsClient();
        StandardEncryptionManagerFactory factory =
                new StandardEncryptionManagerFactory(sharedClient);

        TableMetadata metadata = TableMetadata.buildFromEmpty(3)
                .assignUUID()
                .setLocation("/test/location")
                .setCurrentSchema(TEST_SCHEMA, 1)
                .addPartitionSpec(PartitionSpec.unpartitioned())
                .addSortOrder(SortOrder.unsorted())
                .setProperties(Map.of("encryption.key-id", "test-key-id"))
                .build();

        Optional<EncryptionManager> m1 = factory.create(metadata);
        Optional<EncryptionManager> m2 = factory.create(metadata);
        Optional<EncryptionManager> m3 = factory.create(metadata);

        assertThat(m1).as("create() must return a manager for encrypted tables").isPresent();
        assertThat(m2).isPresent();
        assertThat(m3).isPresent();
        assertThat(sharedClient.initializeCalls())
                .as("create() must never invoke initialize() on the shared client")
                .isZero();
        assertThat(factory.sharedKmsClientForTesting())
                .as("factory keeps the same shared client across create() calls")
                .isSameAs(sharedClient);
    }

    @Test
    public void plaintextTableReturnsNullAndDoesNotTouchSharedClient()
    {
        TestableGcpKmsClient sharedClient = new TestableGcpKmsClient();
        StandardEncryptionManagerFactory factory =
                new StandardEncryptionManagerFactory(sharedClient);

        TableMetadata plaintext = TableMetadata.newTableMetadata(
                TEST_SCHEMA,
                PartitionSpec.unpartitioned(),
                "/test/location",
                Map.of());

        Optional<EncryptionManager> m = factory.create(plaintext);

        assertThat(m).as("plaintext tables must return empty Optional").isEmpty();
        assertThat(sharedClient.wrapCalls()).isZero();
        assertThat(sharedClient.unwrapCalls()).isZero();
    }

    @Test
    public void closeOnHierarchicalChainDoesNotPropagateToSharedClient()
    {
        TestableGcpKmsClient sharedClient = new TestableGcpKmsClient();
        StandardEncryptionManagerFactory factory =
                new StandardEncryptionManagerFactory(sharedClient);

        HierarchicalKeyManagementClient chain = new HierarchicalKeyManagementClient(
                new NonClosingKmsWrapper(factory.sharedKmsClientForTesting()),
                Map.of(),
                "table-key-id");

        chain.close();
        chain.close();
        chain.close();

        assertThat(sharedClient.closeCalls())
                .as("close() through HKMC must not propagate to the shared client")
                .isZero();
    }
}
