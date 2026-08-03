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

import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;

import java.util.Map;

import static java.util.Objects.requireNonNull;

/**
 * Simple FileIO wrapper that ensures properties() returns the configuration.
 * This is used as the base FileIO before passing to EncryptingFileIO.combine().
 */
public class PropertyExposingFileIO
        implements FileIO
{
    private final FileIO delegate;
    private final Map<String, String> properties;

    public PropertyExposingFileIO(FileIO delegate)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
        this.properties = delegate.properties();
    }

    @Override
    public InputFile newInputFile(String path)
    {
        return delegate.newInputFile(path);
    }

    @Override
    public InputFile newInputFile(String path, long length)
    {
        return delegate.newInputFile(path, length);
    }

    @Override
    public OutputFile newOutputFile(String path)
    {
        return delegate.newOutputFile(path);
    }

    @Override
    public void deleteFile(String path)
    {
        delegate.deleteFile(path);
    }

    @Override
    public Map<String, String> properties()
    {
        return properties;
    }

    @Override
    public void close()
    {
        delegate.close();
    }
}
