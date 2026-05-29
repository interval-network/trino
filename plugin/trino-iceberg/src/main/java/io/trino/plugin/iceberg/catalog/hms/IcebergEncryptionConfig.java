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

public class IcebergEncryptionConfig
{
    private String kmsImpl;
    private String kmsKeyUri;

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
}
