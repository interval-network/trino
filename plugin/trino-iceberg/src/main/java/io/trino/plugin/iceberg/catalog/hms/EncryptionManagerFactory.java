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

import java.util.Optional;

/**
 * Factory for creating EncryptionManager instances from table metadata.
 */
public interface EncryptionManagerFactory
{
    /**
     * Returns an EncryptionManager for the given table metadata, or empty if the table is not encrypted.
     */
    Optional<EncryptionManager> create(TableMetadata metadata);
}
