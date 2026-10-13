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
package org.apache.iceberg.rest;

import com.google.common.collect.ImmutableMap;
import io.trino.plugin.iceberg.catalog.rest.IcebergRestCatalogConfig;
import io.trino.plugin.iceberg.catalog.rest.IcebergRestCatalogPropertiesProvider;
import io.trino.spi.NodeVersion;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/// Lives in Iceberg's package to reach HTTPClient's package-private property names, so an Iceberg
/// upgrade that renames them, or changes their defaults, fails here instead of silently turning
/// iceberg.rest-catalog.max-connections(-per-route) into no-ops.
final class TestTrinoRestCatalogHttpClientProperties
{
    @Test
    void testPoolPropertiesMatchHttpClient()
    {
        Map<String, String> properties = new IcebergRestCatalogPropertiesProvider(
                new IcebergRestCatalogConfig().setBaseUri("http://localhost:1234"),
                ImmutableMap::of,
                new NodeVersion("test"))
                .catalogProperties();

        assertThat(properties)
                .containsEntry(HTTPClient.REST_MAX_CONNECTIONS, String.valueOf(HTTPClient.REST_MAX_CONNECTIONS_DEFAULT))
                .containsEntry(HTTPClient.REST_MAX_CONNECTIONS_PER_ROUTE, String.valueOf(HTTPClient.REST_MAX_CONNECTIONS_PER_ROUTE_DEFAULT));
    }
}
