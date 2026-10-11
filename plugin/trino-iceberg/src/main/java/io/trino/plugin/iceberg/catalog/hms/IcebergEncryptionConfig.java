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

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.units.Duration;
import io.airlift.units.MinDuration;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;

import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

public class IcebergEncryptionConfig
{
    private String kmsImpl;
    private String kmsKeyUri;
    // Interval fork (PLE-552): cache of KMS-unwrapped keys shared across queries. 0s = off.
    private Duration keyCacheTtl = new Duration(0, SECONDS);
    private long keyCacheMaxSize = 1000;

    public String getKmsImpl()
    {
        return kmsImpl;
    }

    @Config("encryption.kms-impl")
    @ConfigDescription("KMS implementation class for Iceberg table encryption")
    public IcebergEncryptionConfig setKmsImpl(String kmsImpl)
    {
        this.kmsImpl = kmsImpl;
        return this;
    }

    public String getKmsKeyUri()
    {
        return kmsKeyUri;
    }

    @Config("encryption.kms.key-uri")
    @ConfigDescription("KMS key URI for Iceberg table encryption (e.g., gcp-kms://projects/.../cryptoKeys/...)")
    public IcebergEncryptionConfig setKmsKeyUri(String kmsKeyUri)
    {
        this.kmsKeyUri = kmsKeyUri;
        return this;
    }

    @NotNull
    @MinDuration("0s")
    public Duration getKeyCacheTtl()
    {
        return keyCacheTtl;
    }

    @Config("encryption.kms.key-cache-ttl")
    @ConfigDescription("How long unwrapped KMS key material is reused across queries; 0s disables the cache. " +
            "A disabled or revoked KMS key keeps decrypting on this node for up to this long")
    public IcebergEncryptionConfig setKeyCacheTtl(Duration keyCacheTtl)
    {
        this.keyCacheTtl = keyCacheTtl;
        return this;
    }

    @Min(1)
    public long getKeyCacheMaxSize()
    {
        return keyCacheMaxSize;
    }

    @Config("encryption.kms.key-cache-max-size")
    @ConfigDescription("Maximum number of unwrapped keys kept by encryption.kms.key-cache-ttl")
    public IcebergEncryptionConfig setKeyCacheMaxSize(long keyCacheMaxSize)
    {
        this.keyCacheMaxSize = keyCacheMaxSize;
        return this;
    }

    public boolean isKeyCacheEnabled()
    {
        return keyCacheTtl.roundTo(NANOSECONDS) > 0;
    }
}
