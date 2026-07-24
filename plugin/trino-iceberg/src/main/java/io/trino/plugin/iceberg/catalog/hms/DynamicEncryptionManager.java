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

import org.apache.iceberg.encryption.EncryptedInputFile;
import org.apache.iceberg.encryption.EncryptedOutputFile;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.encryption.PlaintextEncryptionManager;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;

import static java.util.Objects.requireNonNull;

/**
 * Dynamic EncryptionManager that can be upgraded from PlaintextEncryptionManager
 * to StandardEncryptionManager after table metadata is loaded.
 *
 * This solves the timing issue where Snapshots capture the EncryptionManager reference
 * before we know if the table is encrypted. By using this dynamic wrapper, Snapshots
 * get the correct encryption behavior even though they were created before encryption
 * was detected.
 */
public class DynamicEncryptionManager
        implements EncryptionManager
{
    private volatile EncryptionManager delegate;

    public DynamicEncryptionManager()
    {
        this.delegate = PlaintextEncryptionManager.instance();
    }

    public void upgrade(EncryptionManager encryptionManager)
    {
        this.delegate = requireNonNull(encryptionManager, "encryptionManager is null");
    }

    @Override
    public InputFile decrypt(EncryptedInputFile encryptedFile)
    {
        return delegate.decrypt(encryptedFile);
    }

    @Override
    public EncryptedOutputFile encrypt(OutputFile rawFile)
    {
        return delegate.encrypt(rawFile);
    }

    /**
     * Get the current delegate EncryptionManager.
     * This is needed for Iceberg utility methods that check instanceof StandardEncryptionManager.
     */
    public EncryptionManager getDelegate()
    {
        return delegate;
    }
}
