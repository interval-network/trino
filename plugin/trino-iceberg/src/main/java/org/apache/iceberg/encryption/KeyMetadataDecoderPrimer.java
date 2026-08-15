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

import io.airlift.log.Logger;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Set;

/**
 * Forces {@link StandardKeyMetadata}'s static Avro decoder to be built under this plugin's
 * classloader, at a moment we control, instead of on whichever thread happens to perform the first
 * decrypt.
 *
 * <p><b>The bug this prevents (DAT-1126).</b> {@code StandardKeyMetadata.KEY_METADATA_DECODER} is a
 * static singleton. {@code KeyMetadataDecoder.decode} lazily builds one {@code RawDecoder} per
 * write-schema version on the first decode and caches it <em>forever</em>. Building it calls
 * {@code GenericAvroReader.create}, which captures {@code Thread.currentThread().getContextClassLoader()}
 * as the loader it will use to resolve the record class. Resolution goes through {@code DynClasses},
 * which tries only that loader and swallows {@code ClassNotFoundException}; on failure
 * {@code GenericAvroReader} <em>silently</em> falls back to a plain generic-record reader.
 *
 * <p>Inside Trino these classes live in a catalog-scoped {@code PluginClassLoader}, and the
 * {@code iceberg-split-source-<catalog>} executor is a lazily-populated cached pool whose threads
 * inherit the context classloader of whatever thread triggered their creation. When that is not the
 * plugin loader, the generic fallback is cached and every subsequent decode throws
 * {@code ClassCastException: org.apache.avro.generic.GenericData$Record cannot be cast to
 * org.apache.iceberg.encryption.StandardKeyMetadata} — on every thread, for the life of the JVM.
 * It is decided per JVM boot, so a healthy boot is not evidence that the problem is gone.
 *
 * <p><b>The fix.</b> Encode a throwaway {@link StandardKeyMetadata} and parse it back while the
 * context classloader is pinned to this plugin's loader. That one successful decode populates the
 * static cache with a correctly-resolved reader, and the cache is never invalidated.
 *
 * <p>This class must live in {@code org.apache.iceberg.encryption}: {@link StandardKeyMetadata}, its
 * constructors and {@link StandardKeyMetadata#parse} are all package-private. The fork already
 * places {@link TrinoEncryptingFileIO} here for the same reason.
 *
 * <p>Same disease, and the same remedy, as the {@code GcpKeyManagementClient$ByteStringShim}
 * force-initialisation in {@code StandardEncryptionManagerFactory} — that one also exists because a
 * {@code DynClasses} lookup captures the context classloader at an uncontrolled moment. This one
 * fails loudly rather than warning, because a catalog that cannot decode key metadata cannot read
 * any encrypted table: refusing to start is diagnosable in seconds, whereas the warning-and-continue
 * form of this failure went unnoticed for three days.
 */
public final class KeyMetadataDecoderPrimer
{
    private static final Logger log = Logger.get(KeyMetadataDecoderPrimer.class);

    // Arbitrary throwaway values. Never used to encrypt anything — this buffer is encoded and
    // immediately decoded in-process purely to populate the decoder cache. 16 bytes matches the
    // default data key length, so the probe exercises a realistic payload size.
    private static final byte[] PROBE_KEY = new byte[16];
    private static final byte[] PROBE_AAD_PREFIX = new byte[16];

    private KeyMetadataDecoderPrimer() {}

    /**
     * Primes the decoder for every key-metadata schema version this Iceberg build supports.
     *
     * <p>Idempotent and cheap: an in-memory Avro round trip per schema version. Safe to call once
     * per catalog — each catalog's {@code PluginClassLoader} has its own copy of
     * {@link StandardKeyMetadata} and therefore its own static decoder cache to prime.
     *
     * @throws IllegalStateException if the round trip does not produce a {@link StandardKeyMetadata},
     *         which means encrypted reads on this catalog would fail. Fail closed rather than let the
     *         catalog serve a decoder that throws on the first real manifest.
     */
    public static void prime()
    {
        // The loader that defines this class also defines StandardKeyMetadata: same package, same
        // plugin jar. Deriving it here rather than accepting it as a parameter removes any chance of
        // being handed the wrong loader, which would prime the cache with the very fallback reader
        // this exists to prevent.
        ClassLoader pluginClassLoader = KeyMetadataDecoderPrimer.class.getClassLoader();

        // Do not trust the ambient context classloader. IcebergConnectorFactory.create wraps injector
        // creation in a ThreadContextClassLoader, but the ByteStringShim workaround alongside this one
        // documents observing the system loader at @Inject time. Setting it explicitly costs nothing
        // and removes the question — an inert fix here is indistinguishable from no fix at all.
        ClassLoader originalContextClassLoader = Thread.currentThread().getContextClassLoader();
        Thread.currentThread().setContextClassLoader(pluginClassLoader);
        try {
            Set<Byte> schemaVersions = StandardKeyMetadata.supportedSchemaVersions().keySet();
            for (Byte schemaVersion : schemaVersions) {
                primeSchemaVersion(schemaVersion);
            }
            log.info("Primed StandardKeyMetadata decoder for schema version(s) %s under plugin classloader %s",
                    schemaVersions,
                    pluginClassLoader);
        }
        finally {
            Thread.currentThread().setContextClassLoader(originalContextClassLoader);
        }
    }

    /**
     * {@code KeyMetadataDecoder} caches one reader per <em>write</em> schema version, so priming the
     * version this build writes does not cover a manifest written at another version. Today
     * {@link StandardKeyMetadata} defines only V1 and {@link StandardKeyMetadata#buffer()} can only
     * encode at that version, so this loop primes exactly one entry; the loop and the
     * {@code unsupportedSchemaVersion} branch exist so that a future Iceberg bump adding V2 fails
     * here — visibly, at startup — rather than silently reintroducing DAT-1126 for whichever tables
     * happen to carry the new version.
     */
    private static void primeSchemaVersion(byte schemaVersion)
    {
        ByteBuffer encoded = new StandardKeyMetadata(PROBE_KEY, PROBE_AAD_PREFIX).buffer();
        byte encodedVersion = encoded.duplicate().get();
        if (encodedVersion != schemaVersion) {
            throw new IllegalStateException(
                    "Cannot prime the StandardKeyMetadata decoder for schema version " + schemaVersion +
                            ": this Iceberg build encodes at version " + encodedVersion + " and offers no way to " +
                            "produce a version-" + schemaVersion + " buffer. Encrypted reads of tables written at " +
                            "version " + schemaVersion + " would hit the uninitialised decoder and fail with a " +
                            "ClassCastException (DAT-1126). A primer for the new version is needed.");
        }

        StandardKeyMetadata parsed;
        try {
            parsed = StandardKeyMetadata.parse(encoded);
        }
        catch (RuntimeException e) {
            // A ClassCastException here means the static decoder was already poisoned before this ran,
            // and nothing short of a JVM restart can clear it. Surface it rather than let every
            // encrypted scan fail later with the same exception and no explanation.
            throw new IllegalStateException(
                    "Failed to prime the StandardKeyMetadata decoder for schema version " + schemaVersion +
                            " under the plugin classloader. Encrypted Iceberg reads would fail on this catalog " +
                            "(DAT-1126).",
                    e);
        }

        // Guard against a reader that resolves but decodes wrongly: a silent value corruption in the
        // key material would be far worse than a ClassCastException.
        if (!parsed.encryptionKey().equals(ByteBuffer.wrap(PROBE_KEY))
                || !parsed.aadPrefix().equals(ByteBuffer.wrap(PROBE_AAD_PREFIX))) {
            throw new IllegalStateException(
                    "Priming round trip of StandardKeyMetadata at schema version " + schemaVersion +
                            " did not preserve the key material. Refusing to start a catalog whose key-metadata " +
                            "decoder is not faithful (DAT-1126).");
        }
    }

    /**
     * The schema versions this Iceberg build knows how to decode. Exposed for tests that assert the
     * primer's coverage against Iceberg's own list.
     */
    static Map<Byte, org.apache.iceberg.Schema> supportedSchemaVersions()
    {
        return StandardKeyMetadata.supportedSchemaVersions();
    }
}
