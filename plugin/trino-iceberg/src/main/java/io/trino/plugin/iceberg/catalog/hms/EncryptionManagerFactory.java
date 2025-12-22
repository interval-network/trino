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

import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.encryption.EncryptionManager;

/**
 * Factory for creating EncryptionManager instances from table metadata.
 * This factory is responsible for initializing the appropriate encryption
 * manager based on table's encryption configuration.
 */
public interface EncryptionManagerFactory
{
    /**
     * Creates an EncryptionManager for the given table metadata.
     * Returns null if the table is not encrypted.
     *
     * @param metadata the table metadata containing encryption keys
     * @return an EncryptionManager instance, or null if table is not encrypted
     */
    EncryptionManager create(TableMetadata metadata);
}
