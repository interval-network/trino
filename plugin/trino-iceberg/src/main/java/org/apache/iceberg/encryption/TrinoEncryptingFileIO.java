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
 * checks `instanceof StandardEncryptionManager` on the EncryptionManager object.
 */
public class TrinoEncryptingFileIO
        extends EncryptingFileIO
{
    private final FileIO baseFileIO;
    private final EncryptionManager encryptionManager;

    /**
     * Package-private constructor accessible because we're in the same package as EncryptingFileIO.
     */
    TrinoEncryptingFileIO(FileIO fileIO, EncryptionManager encryptionManager, FileIO baseFileIO)
    {
        super(fileIO, encryptionManager);
        this.baseFileIO = requireNonNull(baseFileIO, "baseFileIO is null");
        this.encryptionManager = requireNonNull(encryptionManager, "encryptionManager is null");
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

    private EncryptionManager getEffectiveManager()
    {
        // If using DynamicEncryptionManager, return its delegate for instanceof checks
        if (encryptionManager instanceof DynamicEncryptionManager) {
            return ((DynamicEncryptionManager) encryptionManager).getDelegate();
        }
        return encryptionManager;
    }

    @Override
    public InputFile newInputFile(String location)
    {
        EncryptingFileIO effectiveFileIO = new EncryptingFileIO(baseFileIO, getEffectiveManager());
        return effectiveFileIO.newInputFile(location);
    }

    @Override
    public InputFile newInputFile(String location, long length)
    {
        EncryptingFileIO effectiveFileIO = new EncryptingFileIO(baseFileIO, getEffectiveManager());
        return effectiveFileIO.newInputFile(location, length);
    }

    @Override
    public InputFile newInputFile(org.apache.iceberg.DataFile dataFile)
    {
        EncryptingFileIO effectiveFileIO = new EncryptingFileIO(baseFileIO, getEffectiveManager());
        return effectiveFileIO.newInputFile(dataFile);
    }

    @Override
    public InputFile newInputFile(org.apache.iceberg.DeleteFile deleteFile)
    {
        EncryptingFileIO effectiveFileIO = new EncryptingFileIO(baseFileIO, getEffectiveManager());
        return effectiveFileIO.newInputFile(deleteFile);
    }

    @Override
    public InputFile newInputFile(org.apache.iceberg.ManifestFile manifestFile)
    {
        EncryptingFileIO effectiveFileIO = new EncryptingFileIO(baseFileIO, getEffectiveManager());
        return effectiveFileIO.newInputFile(manifestFile);
    }

    @Override
    public InputFile newInputFile(ManifestListFile manifestListFile)
    {
        EncryptionManager effectiveManager = getEffectiveManager();
        EncryptingFileIO effectiveFileIO = new EncryptingFileIO(baseFileIO, effectiveManager);
        return effectiveFileIO.newInputFile(manifestListFile);
    }
}
