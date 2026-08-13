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

import com.google.common.annotations.VisibleForTesting;
import com.google.inject.Inject;
import io.trino.plugin.iceberg.catalog.hms.StandardEncryptionManagerFactory;
import io.trino.plugin.iceberg.encryption.EncryptionManagerFactory;
import io.trino.spi.TrinoException;
import org.apache.iceberg.encryption.EncryptedKey;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.encryption.EncryptionUtil;
import org.apache.iceberg.encryption.KeyManagementClient;
import org.apache.iceberg.encryption.PlaintextEncryptionManager;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.trino.plugin.iceberg.IcebergErrorCode.ICEBERG_CATALOG_ERROR;
import static java.util.Objects.requireNonNull;
import static org.apache.iceberg.TableProperties.ENCRYPTION_TABLE_KEY;

/**
 * Serves Trino's <em>native</em> {@link EncryptionManagerFactory} contract from the fork's
 * REST-catalog KMS client, so that both encryption subsystems share one key management client.
 *
 * <p><b>Why this exists.</b> Trino 483 added a native Iceberg encryption framework configured by
 * {@code iceberg.encryption.kms-type} — a closed {@code AWS|AZURE|GCP} enum. Our tables are wrapped
 * by the Iceberg <em>library</em> encryption path via the fork, configured by the bare
 * {@code encryption.kms-impl} / {@code encryption.kms.key-uri} properties. Ordinary data reads never
 * meet the native framework: {@code EncryptionAwareRESTSessionCatalog} attaches an EncryptionManager
 * at table load and the split path uses it. But the {@code $files} metadata table takes a different
 * route — {@code IcebergPageSourceProvider}'s {@code FilesTableSplit} branch asks the <em>native</em>
 * factory for a manager — and {@code DefaultEncryptionManagerFactory} throws there for any table
 * carrying {@code encryption.key-id} unless {@code iceberg.encryption.kms-type} is set. That is what
 * broke {@code $files} and {@code $partitions} (a SQL view over {@code $files}) on 483 while ordinary
 * SELECTs kept working.
 *
 * <p>Setting {@code iceberg.encryption.kms-type=GCP} would resolve the same
 * {@code org.apache.iceberg.gcp.GcpKeyManagementClient} class, but it would stand up a
 * <em>second</em>, independently configured KMS client, and the closed enum cannot express the
 * OpenBao client used by the on-prem profiles. Reusing the fork's already-initialised client keeps
 * one code path for both.
 *
 * <p><b>Fail-closed.</b> The semantics below deliberately mirror
 * {@code DefaultEncryptionManagerFactory}: plaintext <em>only</em> when the table declares no
 * {@link org.apache.iceberg.TableProperties#ENCRYPTION_TABLE_KEY}, and a hard failure when the table
 * is encrypted but no key management client is configured. Returning a
 * {@link PlaintextEncryptionManager} in that second case would let an encrypted table be treated as
 * plaintext — the silent downgrade this class exists to prevent.
 */
public class RestNativeEncryptionManagerFactory
        implements EncryptionManagerFactory
{
    private final Optional<KeyManagementClient> keyManagementClient;

    @Inject
    public RestNativeEncryptionManagerFactory(StandardEncryptionManagerFactory forkFactory)
    {
        this(requireNonNull(forkFactory, "forkFactory is null").sharedKmsClient());
    }

    @VisibleForTesting
    RestNativeEncryptionManagerFactory(Optional<KeyManagementClient> keyManagementClient)
    {
        this.keyManagementClient = requireNonNull(keyManagementClient, "keyManagementClient is null");
    }

    @Override
    public EncryptionManager create(List<EncryptedKey> encryptionKeys, Map<String, String> tableProperties)
    {
        requireNonNull(encryptionKeys, "encryptionKeys is null");
        requireNonNull(tableProperties, "tableProperties is null");

        String tableKeyId = tableProperties.get(ENCRYPTION_TABLE_KEY);
        // The ONLY plaintext branch: the table genuinely declares no encryption key at all.
        // Matches DefaultEncryptionManagerFactory:55-57.
        if (tableKeyId == null) {
            return PlaintextEncryptionManager.instance();
        }

        // A present-but-blank key id is malformed, not "unencrypted". Treating it as plaintext would
        // silently downgrade a table that declares encryption; failing here surfaces it instead of
        // deferring to an obscure error at the first KMS unwrap.
        if (tableKeyId.isBlank()) {
            throw new TrinoException(
                    ICEBERG_CATALOG_ERROR,
                    "Table property " + ENCRYPTION_TABLE_KEY + " is present but blank. Refusing to read a table that " +
                            "declares encryption as plaintext.");
        }

        KeyManagementClient kmsClient = keyManagementClient.orElseThrow(() -> new TrinoException(
                ICEBERG_CATALOG_ERROR,
                "Table declares " + ENCRYPTION_TABLE_KEY + " but no key management client is configured for this " +
                        "REST catalog. Set encryption.kms-impl (and encryption.kms.key-uri for GCP KMS). Refusing to " +
                        "read an encrypted table as plaintext."));
        return EncryptionUtil.createEncryptionManager(encryptionKeys, tableProperties, kmsClient);
    }
}
