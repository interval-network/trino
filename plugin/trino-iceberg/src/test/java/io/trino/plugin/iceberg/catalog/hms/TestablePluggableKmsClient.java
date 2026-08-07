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

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Test double for the pluggable-KMS path: a KeyManagementClient that is deliberately NOT a
 * GcpKeyManagementClient, so selecting it proves {@code encryption.kms-impl} was honored rather
 * than silently ignored. Needs a public no-arg constructor to be reflectively instantiable.
 *
 * <p>State is per-instance, not static: these tests run in parallel, so a static counter would
 * race across tests that each instantiate their own client.
 */
public class TestablePluggableKmsClient
        implements KeyManagementClient
{
    private final AtomicInteger initializeCalls = new AtomicInteger();
    private volatile Map<String, String> initializeProperties;

    public int initializeCalls()
    {
        return initializeCalls.get();
    }

    public Map<String, String> initializeProperties()
    {
        return initializeProperties;
    }

    @Override
    public ByteBuffer wrapKey(ByteBuffer key, String wrappingKeyId)
    {
        byte[] data = new byte[key.remaining()];
        key.get(data);
        return ByteBuffer.wrap(data);
    }

    @Override
    public ByteBuffer unwrapKey(ByteBuffer wrappedKey, String wrappingKeyId)
    {
        byte[] data = new byte[wrappedKey.remaining()];
        wrappedKey.get(data);
        return ByteBuffer.wrap(data);
    }

    @Override
    public void initialize(Map<String, String> properties)
    {
        initializeCalls.incrementAndGet();
        initializeProperties = Map.copyOf(properties);
    }
}
