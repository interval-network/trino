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
package org.apache.iceberg.encryption;

import io.trino.plugin.iceberg.catalog.hms.DynamicEncryptionManager;
import org.apache.iceberg.ManifestListFile;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;

import java.util.Map;

import static java.util.Objects.requireNonNull;

/**
 * Extension of EncryptingFileIO that exposes properties() from the base FileIO
 * and handles DynamicEncryptionManager by using its delegate for instanceof checks.
 *
 * This is needed because Iceberg's EncryptionUtil.decryptManifestListKeyMetadata()
 * checks {@code instanceof StandardEncryptionManager} on the EncryptionManager object.
 *
 * The effective EncryptingFileIO (backed by the actual delegate, not the DynamicEncryptionManager
 * wrapper) is cached and only recreated when the delegate changes, which happens at most once
 * per table when DynamicEncryptionManager.upgrade() is called.
 */
public class TrinoEncryptingFileIO
        extends EncryptingFileIO
{
    private final FileIO baseFileIO;
    private final EncryptionManager encryptionManager;

    // Cached effective EncryptingFileIO, invalidated when DynamicEncryptionManager's delegate changes.
    // Uses a single volatile reference to an immutable record so both fields update atomically.
    private record EffectiveState(EncryptionManager delegate, EncryptingFileIO fileIO) {}

    private volatile EffectiveState effectiveState;

    /**
     * Package-private constructor accessible because we're in the same package as EncryptingFileIO.
     */
    TrinoEncryptingFileIO(FileIO fileIO, EncryptionManager encryptionManager, FileIO baseFileIO)
    {
        super(fileIO, encryptionManager);
        this.baseFileIO = requireNonNull(baseFileIO, "baseFileIO is null");
        this.encryptionManager = requireNonNull(encryptionManager, "encryptionManager is null");
        EncryptionManager initial = resolveDelegate(encryptionManager);
        this.effectiveState = new EffectiveState(initial, new EncryptingFileIO(baseFileIO, initial));
    }

    /**
     * Factory method to create TrinoEncryptingFileIO.
     */
    public static TrinoEncryptingFileIO wrap(FileIO fileIO, EncryptionManager encryptionManager, FileIO baseFileIO)
    {
        return new TrinoEncryptingFileIO(fileIO, encryptionManager, baseFileIO);
    }

    @Override
    public Map<String, String> properties()
    {
        // Expose properties from the base FileIO for Trino's IcebergSplitSource
        return baseFileIO.properties();
    }

    private static EncryptionManager resolveDelegate(EncryptionManager manager)
    {
        if (manager instanceof DynamicEncryptionManager) {
            return ((DynamicEncryptionManager) manager).getDelegate();
        }
        return manager;
    }

    private EncryptingFileIO effectiveFileIO()
    {
        EncryptionManager current = resolveDelegate(encryptionManager);
        EffectiveState state = effectiveState;
        if (state.delegate() != current) {
            // DynamicEncryptionManager was upgraded; refresh the cached EncryptingFileIO.
            // Benign race: two threads may both create a new state, but both produce the same result.
            state = new EffectiveState(current, new EncryptingFileIO(baseFileIO, current));
            effectiveState = state;
        }
        return state.fileIO();
    }

    @Override
    public InputFile newInputFile(String location)
    {
        return effectiveFileIO().newInputFile(location);
    }

    @Override
    public InputFile newInputFile(String location, long length)
    {
        return effectiveFileIO().newInputFile(location, length);
    }

    @Override
    public InputFile newInputFile(org.apache.iceberg.DataFile dataFile)
    {
        return effectiveFileIO().newInputFile(dataFile);
    }

    @Override
    public InputFile newInputFile(org.apache.iceberg.DeleteFile deleteFile)
    {
        return effectiveFileIO().newInputFile(deleteFile);
    }

    @Override
    public InputFile newInputFile(org.apache.iceberg.ManifestFile manifestFile)
    {
        return effectiveFileIO().newInputFile(manifestFile);
    }

    @Override
    public InputFile newInputFile(ManifestListFile manifestListFile)
    {
        return effectiveFileIO().newInputFile(manifestListFile);
    }
}
