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

import static java.util.Objects.requireNonNull;

/**
 * Forwards wrapKey/unwrapKey to a shared KeyManagementClient delegate
 * while no-opping close() and initialize().
 *
 * Used by StandardEncryptionManagerFactory to pass a shared GcpKeyManagementClient
 * (a connector-scope singleton owned by the factory) into per-table
 * HierarchicalKeyManagementClient wrappers. HierarchicalKeyManagementClient.close()
 * delegates to its underlying kmsClient.close(); routing through this wrapper
 * ensures any such close call cannot shut down the shared client.
 */
public class NonClosingKmsWrapper
        implements KeyManagementClient
{
    private final KeyManagementClient delegate;

    public NonClosingKmsWrapper(KeyManagementClient delegate)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
    }

    @Override
    public ByteBuffer wrapKey(ByteBuffer key, String wrappingKeyId)
    {
        return delegate.wrapKey(key, wrappingKeyId);
    }

    @Override
    public ByteBuffer unwrapKey(ByteBuffer wrappedKey, String wrappingKeyId)
    {
        return delegate.unwrapKey(wrappedKey, wrappingKeyId);
    }

    @Override
    public void initialize(Map<String, String> properties) {}

    @Override
    public void close() {}
}
