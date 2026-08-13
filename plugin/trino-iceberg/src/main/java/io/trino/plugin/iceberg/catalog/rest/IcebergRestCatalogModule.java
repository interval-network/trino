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

import com.google.inject.Binder;
import com.google.inject.Scopes;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.trino.plugin.iceberg.IcebergConfig;
import io.trino.plugin.iceberg.IcebergFileSystemFactory;
import io.trino.plugin.iceberg.catalog.TrinoCatalogFactory;
import io.trino.plugin.iceberg.catalog.hms.EncryptionManagerFactory;
import io.trino.plugin.iceberg.catalog.hms.IcebergEncryptionConfig;
import io.trino.plugin.iceberg.catalog.hms.StandardEncryptionManagerFactory;
import io.trino.spi.TrinoException;

import static com.google.inject.multibindings.OptionalBinder.newOptionalBinder;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;

public class IcebergRestCatalogModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        configBinder(binder).bindConfig(IcebergRestCatalogConfig.class);
        // PARE encryption: consume the encryption.kms.* catalog properties and bind the
        // GCP-KMS-backed EncryptionManagerFactory used by EncryptionAwareRESTSessionCatalog.
        configBinder(binder).bindConfig(IcebergEncryptionConfig.class);
        install(switch (buildConfigObject(IcebergRestCatalogConfig.class).getSecurity()) {
            case OAUTH2 -> new OAuth2SecurityModule();
            case SIGV4 -> new SigV4SecurityModule();
            case GOOGLE -> new GoogleSecurityModule();
            case NONE -> new NoneSecurityModule();
        });

        binder.bind(IcebergRestCatalogPropertiesProvider.class).in(Scopes.SINGLETON);
        // Bind the CONCRETE fork factory explicitly and as a singleton, then link the fork interface to
        // it. Two reasons, both load-bearing:
        //  1. Trino runs Guice with requireExplicitBindings, so a just-in-time binding is unavailable —
        //     injecting StandardEncryptionManagerFactory into the adapter below fails at injector
        //     creation ([Guice/JitDisabled]) without this.
        //  2. It guarantees the fork interface and the native-interface adapter share ONE instance, and
        //     therefore ONE initialised KeyManagementClient. A separate instance per injection point
        //     would build a second KMS client, which is exactly what this design exists to avoid.
        binder.bind(StandardEncryptionManagerFactory.class).in(Scopes.SINGLETON);
        binder.bind(EncryptionManagerFactory.class).to(StandardEncryptionManagerFactory.class);

        // Serve Trino's NATIVE encryption factory from the fork's KMS client, for REST catalogs only.
        // IcebergModule binds it with .setDefault() -> DefaultEncryptionManagerFactory, which throws for
        // any table carrying encryption.key-id unless iceberg.encryption.kms-type is set; that is what
        // broke "$files"/"$partitions" on 483 (IcebergPageSourceProvider's FilesTableSplit branch) while
        // ordinary data reads, which use the manager attached by EncryptionAwareRESTSessionCatalog, kept
        // working. Overriding here rather than globally leaves hive/glue/nessie/jdbc catalogs on upstream
        // behaviour, since IcebergCatalogModule installs exactly one catalog module per catalog injector.
        // NOTE: a test that combines its own .setBinding() for this key with iceberg.catalog.type=REST
        // would now produce a duplicate binding and fail at injector creation.
        newOptionalBinder(binder, io.trino.plugin.iceberg.encryption.EncryptionManagerFactory.class)
                .setBinding().to(RestNativeEncryptionManagerFactory.class).in(Scopes.SINGLETON);
        binder.bind(TrinoCatalogFactory.class).to(TrinoIcebergRestCatalogFactory.class).in(Scopes.SINGLETON);
        newOptionalBinder(binder, IcebergFileSystemFactory.class).setBinding().to(IcebergRestCatalogFileSystemFactory.class).in(Scopes.SINGLETON);

        IcebergConfig icebergConfig = buildConfigObject(IcebergConfig.class);
        IcebergRestCatalogConfig restCatalogConfig = buildConfigObject(IcebergRestCatalogConfig.class);
        if (restCatalogConfig.isVendedCredentialsEnabled() && icebergConfig.isRegisterTableProcedureEnabled()) {
            throw new TrinoException(NOT_SUPPORTED, "Using the `register_table` procedure with vended credentials is currently not supported");
        }
    }
}
