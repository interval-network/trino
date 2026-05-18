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

import org.apache.iceberg.gcp.GcpKeyManagementClient;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Test double for GcpKeyManagementClient. Does not perform real gRPC calls,
 * does not spawn Gax threads, and exposes counters so tests can assert
 * lifecycle behavior.
 */
public class TestableGcpKmsClient
        extends GcpKeyManagementClient
{
    private final AtomicInteger wrapCalls = new AtomicInteger();
    private final AtomicInteger unwrapCalls = new AtomicInteger();
    private final AtomicInteger initializeCalls = new AtomicInteger();
    private final AtomicInteger closeCalls = new AtomicInteger();

    @Override
    public ByteBuffer wrapKey(ByteBuffer key, String wrappingKeyId)
    {
        wrapCalls.incrementAndGet();
        byte[] data = new byte[key.remaining()];
        key.get(data);
        return ByteBuffer.wrap(data);
    }

    @Override
    public ByteBuffer unwrapKey(ByteBuffer wrappedKey, String wrappingKeyId)
    {
        unwrapCalls.incrementAndGet();
        byte[] data = new byte[wrappedKey.remaining()];
        wrappedKey.get(data);
        return ByteBuffer.wrap(data);
    }

    @Override
    public void initialize(Map<String, String> properties)
    {
        initializeCalls.incrementAndGet();
    }

    @Override
    public void close()
    {
        closeCalls.incrementAndGet();
    }

    public int wrapCalls() { return wrapCalls.get(); }
    public int unwrapCalls() { return unwrapCalls.get(); }
    public int initializeCalls() { return initializeCalls.get(); }
    public int closeCalls() { return closeCalls.get(); }
}
