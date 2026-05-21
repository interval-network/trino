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

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

public class TestNonClosingKmsWrapper
{
    @Test
    public void closeIsNoOp()
    {
        TestableGcpKmsClient inner = new TestableGcpKmsClient();
        NonClosingKmsWrapper wrapper = new NonClosingKmsWrapper(inner);

        wrapper.close();
        wrapper.close();
        wrapper.close();

        assertThat(inner.closeCalls()).isEqualTo(0);
    }

    @Test
    public void initializeIsNoOp()
    {
        TestableGcpKmsClient inner = new TestableGcpKmsClient();
        NonClosingKmsWrapper wrapper = new NonClosingKmsWrapper(inner);

        wrapper.initialize(Map.of("encryption.kms.key-uri", "test-uri"));

        assertThat(inner.initializeCalls()).isEqualTo(0);
    }

    @Test
    public void wrapKeyIsForwarded()
    {
        TestableGcpKmsClient inner = new TestableGcpKmsClient();
        NonClosingKmsWrapper wrapper = new NonClosingKmsWrapper(inner);
        ByteBuffer key = ByteBuffer.wrap(new byte[]{1, 2, 3, 4});

        ByteBuffer result = wrapper.wrapKey(key, "key-id");

        assertThat(inner.wrapCalls()).isEqualTo(1);
        assertThat(result.array()).containsExactly(1, 2, 3, 4);
    }

    @Test
    public void unwrapKeyIsForwarded()
    {
        TestableGcpKmsClient inner = new TestableGcpKmsClient();
        NonClosingKmsWrapper wrapper = new NonClosingKmsWrapper(inner);
        ByteBuffer wrapped = ByteBuffer.wrap(new byte[]{5, 6, 7, 8});

        ByteBuffer result = wrapper.unwrapKey(wrapped, "key-id");

        assertThat(inner.unwrapCalls()).isEqualTo(1);
        assertThat(result.array()).containsExactly(5, 6, 7, 8);
    }
}
