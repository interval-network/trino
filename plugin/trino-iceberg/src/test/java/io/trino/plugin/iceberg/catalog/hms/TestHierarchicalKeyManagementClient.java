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

import org.apache.iceberg.encryption.KeyManagementClient;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

public class TestHierarchicalKeyManagementClient
{
    @Test
    public void wrapKeyForwardsThroughInterfaceTypedDelegate()
    {
        TestableGcpKmsClient backing = new TestableGcpKmsClient();
        // Pass an interface-typed delegate (NOT a GcpKeyManagementClient) to prove
        // the constructor parameter accepts the interface.
        KeyManagementClient delegate = new NonClosingKmsWrapper(backing);

        HierarchicalKeyManagementClient hkmc = new HierarchicalKeyManagementClient(
                delegate,
                Map.of(),
                "test-key-id");

        ByteBuffer key = ByteBuffer.wrap(new byte[]{1, 2, 3, 4});
        hkmc.wrapKey(key, "test-key-id");

        assertThat(backing.wrapCalls()).isEqualTo(1);
    }

    @Test
    public void unwrapKeyMatchingTableKeyIdGoesDirectlyToDelegate()
    {
        TestableGcpKmsClient backing = new TestableGcpKmsClient();
        KeyManagementClient delegate = new NonClosingKmsWrapper(backing);

        HierarchicalKeyManagementClient hkmc = new HierarchicalKeyManagementClient(
                delegate,
                Map.of(),
                "table-key-id");

        ByteBuffer wrapped = ByteBuffer.wrap(new byte[]{5, 6, 7, 8});
        hkmc.unwrapKey(wrapped, "table-key-id");

        assertThat(backing.unwrapCalls()).isEqualTo(1);
    }
}
