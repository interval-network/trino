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

import com.google.common.collect.ImmutableMap;
import io.trino.spi.NodeVersion;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

final class TestIcebergRestCatalogPropertiesProvider
{
    @Test
    void testDefaults()
    {
        Map<String, String> properties = catalogProperties(new IcebergRestCatalogConfig().setBaseUri("http://localhost:1234"));

        assertThat(properties)
                .containsEntry("rest-metrics-reporting-enabled", "false")
                .containsEntry("rest.client.max-connections", "100")
                .containsEntry("rest.client.connections-per-route", "100");
    }

    @Test
    void testExplicitValues()
    {
        Map<String, String> properties = catalogProperties(new IcebergRestCatalogConfig()
                .setBaseUri("http://localhost:1234")
                .setMetricsReportingEnabled(true)
                .setMaxConnections(400)
                .setMaxConnectionsPerRoute(300));

        assertThat(properties)
                .containsEntry("rest-metrics-reporting-enabled", "true")
                .containsEntry("rest.client.max-connections", "400")
                .containsEntry("rest.client.connections-per-route", "300");
    }

    private static Map<String, String> catalogProperties(IcebergRestCatalogConfig config)
    {
        return new IcebergRestCatalogPropertiesProvider(config, ImmutableMap::of, new NodeVersion("test"))
                .catalogProperties();
    }
}
