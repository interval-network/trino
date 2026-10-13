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

import com.google.common.collect.ImmutableMap;
import io.airlift.units.Duration;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;

// The fork's REST-path encryption properties (encryption.kms.*), not Trino's native iceberg.encryption.* config
final class TestForkIcebergEncryptionConfig
{
    @Test
    void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(IcebergEncryptionConfig.class)
                .setKmsImpl(null)
                .setKmsKeyUri(null)
                .setKeyCacheTtl(new Duration(0, SECONDS))
                .setKeyCacheMaxSize(1000));
        // PLE-552: the unwrapped-key cache is off unless an environment opts in
        assertThat(new IcebergEncryptionConfig().isKeyCacheEnabled()).isFalse();
    }

    @Test
    void testExplicitPropertyMappings()
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("encryption.kms-impl", "org.example.Kms")
                .put("encryption.kms.key-uri", "gcp-kms://projects/p/locations/l/keyRings/k/cryptoKeys/c")
                .put("encryption.kms.key-cache-ttl", "300s")
                .put("encryption.kms.key-cache-max-size", "50")
                .buildOrThrow();

        assertFullMapping(properties, new IcebergEncryptionConfig()
                .setKmsImpl("org.example.Kms")
                .setKmsKeyUri("gcp-kms://projects/p/locations/l/keyRings/k/cryptoKeys/c")
                .setKeyCacheTtl(new Duration(300, SECONDS))
                .setKeyCacheMaxSize(50));
    }
}
