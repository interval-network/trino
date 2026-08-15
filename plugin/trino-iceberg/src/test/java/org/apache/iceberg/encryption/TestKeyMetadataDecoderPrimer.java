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

import org.apache.iceberg.Schema;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.ByteBuffer;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression test for DAT-1126: {@code StandardKeyMetadata}'s static Avro decoder is built once, on
 * the first decode, using whatever thread context classloader is current at that moment — and cached
 * for the life of the JVM. Under Trino's {@code PluginClassLoader}, one decode on a thread carrying
 * the wrong context classloader poisons every encrypted read on that node until restart.
 *
 * <p><b>Why the classloader gymnastics.</b> The poisoned state lives in a {@code private static
 * final} field, so it cannot be reset between tests in one classloader. Each test therefore gets a
 * fresh {@link IsolatedEncryptionPackageClassLoader} that re-defines
 * {@code org.apache.iceberg.encryption.*} itself, which both hands the test a virgin decoder cache
 * and faithfully models the production situation — the encryption classes reachable from one
 * classloader and not another.
 *
 * <p>{@link #firstDecodeOnAForeignContextClassLoaderPoisonsTheDecoder()} is the control. It must
 * fail in the same way production did; without it passing, the primer test proves nothing.
 */
public class TestKeyMetadataDecoderPrimer
{
    private static final String STANDARD_KEY_METADATA = "org.apache.iceberg.encryption.StandardKeyMetadata";
    private static final String PRIMER = "org.apache.iceberg.encryption.KeyMetadataDecoderPrimer";

    private static final byte[] KEY = new byte[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16};
    private static final byte[] AAD_PREFIX = new byte[] {17, 18, 19, 20, 21, 22, 23, 24};

    /**
     * The control, and the reproduction of the live incident. A first decode performed while the
     * context classloader cannot see {@code StandardKeyMetadata} caches a generic-record reader, and
     * the cast at {@code KeyMetadataDecoder.decode} then fails — with the exact exception seen in
     * interval-integration-dev.
     */
    @Test
    public void firstDecodeOnAForeignContextClassLoaderPoisonsTheDecoder()
            throws Exception
    {
        IsolatedEncryptionPackageClassLoader isolated = new IsolatedEncryptionPackageClassLoader();
        Class<?> keyMetadata = isolated.loadClass(STANDARD_KEY_METADATA);
        ByteBuffer encoded = encode(keyMetadata);

        // The platform classloader cannot see any Iceberg class, which is what makes DynClasses'
        // lookup fail and GenericAvroReader fall back to a plain generic-record reader.
        assertThatThrownBy(() -> withContextClassLoader(ClassLoader.getPlatformClassLoader(), () -> parse(keyMetadata, encoded)))
                .isInstanceOf(ClassCastException.class)
                .hasMessageContaining("org.apache.avro.generic.GenericData$Record")
                .hasMessageContaining("org.apache.iceberg.encryption.StandardKeyMetadata");
    }

    /**
     * The fix. Priming under the plugin classloader first means the cached reader resolves the record
     * class, so a later decode on a thread carrying a useless context classloader still succeeds —
     * which is precisely the {@code iceberg-split-source-*} pool's situation.
     */
    @Test
    public void primingFirstSurvivesAForeignContextClassLoader()
            throws Exception
    {
        IsolatedEncryptionPackageClassLoader isolated = new IsolatedEncryptionPackageClassLoader();
        Class<?> keyMetadata = isolated.loadClass(STANDARD_KEY_METADATA);
        Class<?> primer = isolated.loadClass(PRIMER);
        ByteBuffer encoded = encode(keyMetadata);

        // Prime from a thread whose context classloader is useless, to prove the primer sets its own
        // rather than relying on the ambient one.
        withContextClassLoader(ClassLoader.getPlatformClassLoader(), () -> {
            primer.getMethod("prime").invoke(null);
            return null;
        });

        Object parsed = withContextClassLoader(
                ClassLoader.getPlatformClassLoader(),
                () -> parse(keyMetadata, encoded));

        assertThat(parsed).isInstanceOf(keyMetadata);
        assertThat(invokeBuffer(parsed, "encryptionKey")).isEqualTo(ByteBuffer.wrap(KEY));
        assertThat(invokeBuffer(parsed, "aadPrefix")).isEqualTo(ByteBuffer.wrap(AAD_PREFIX));
    }

    /**
     * Priming covers one entry per key-metadata schema version, and {@code buffer()} can only encode
     * at the version this Iceberg build writes. If a future bump adds V2, tables written at V2 would
     * decode through an unprimed cache entry and reintroduce DAT-1126. This test fails at that bump
     * so the primer gets extended deliberately.
     */
    @Test
    public void icebergStillDefinesExactlyOneKeyMetadataSchemaVersion()
    {
        Map<Byte, Schema> versions = KeyMetadataDecoderPrimer.supportedSchemaVersions();
        assertThat(versions.keySet())
                .as("A new key-metadata schema version needs a matching primer — see KeyMetadataDecoderPrimer")
                .containsExactly((byte) 1);
    }

    /**
     * The primer must succeed on a healthy classloader without disturbing the caller's context.
     */
    @Test
    public void primingRestoresTheContextClassLoader()
    {
        ClassLoader sentinel = new URLClassLoader(new URL[0], ClassLoader.getPlatformClassLoader());
        ClassLoader original = Thread.currentThread().getContextClassLoader();
        Thread.currentThread().setContextClassLoader(sentinel);
        try {
            KeyMetadataDecoderPrimer.prime();
            assertThat(Thread.currentThread().getContextClassLoader()).isSameAs(sentinel);
        }
        finally {
            Thread.currentThread().setContextClassLoader(original);
        }
    }

    private static ByteBuffer encode(Class<?> keyMetadata)
            throws Exception
    {
        // Encoding uses a separate static encoder and does not touch the decoder cache, so building
        // the probe buffer cannot itself prime or poison anything.
        var constructor = keyMetadata.getDeclaredConstructor(byte[].class, byte[].class);
        constructor.setAccessible(true);
        Object instance = constructor.newInstance(KEY, AAD_PREFIX);
        Method buffer = keyMetadata.getDeclaredMethod("buffer");
        buffer.setAccessible(true);
        return (ByteBuffer) buffer.invoke(instance);
    }

    private static Object parse(Class<?> keyMetadata, ByteBuffer encoded)
            throws Exception
    {
        Method parse = keyMetadata.getDeclaredMethod("parse", ByteBuffer.class);
        parse.setAccessible(true);
        return parse.invoke(null, encoded.duplicate());
    }

    private static ByteBuffer invokeBuffer(Object instance, String methodName)
            throws Exception
    {
        Method method = instance.getClass().getDeclaredMethod(methodName);
        method.setAccessible(true);
        return (ByteBuffer) method.invoke(instance);
    }

    /**
     * Runs {@code action} with {@code classLoader} as the context classloader, unwrapping reflection.
     */
    private static <T> T withContextClassLoader(ClassLoader classLoader, ThrowingSupplier<T> action)
            throws Exception
    {
        ClassLoader original = Thread.currentThread().getContextClassLoader();
        Thread.currentThread().setContextClassLoader(classLoader);
        try {
            return action.get();
        }
        catch (InvocationTargetException e) {
            if (e.getCause() instanceof Exception cause) {
                throw cause;
            }
            throw e;
        }
        finally {
            Thread.currentThread().setContextClassLoader(original);
        }
    }

    private interface ThrowingSupplier<T>
    {
        T get()
                throws Exception;
    }

    /**
     * Defines every {@code org.apache.iceberg.encryption.*} class itself and delegates the rest to
     * the application classloader. Because {@code StandardKeyMetadata} is re-defined here, its
     * {@code private static final} decoder cache starts empty in each instance — the isolation the
     * poisoning tests need, and a fair model of a Trino {@code PluginClassLoader}.
     */
    private static final class IsolatedEncryptionPackageClassLoader
            extends ClassLoader
    {
        private static final String ISOLATED_PACKAGE = "org.apache.iceberg.encryption.";

        private IsolatedEncryptionPackageClassLoader()
        {
            super(TestKeyMetadataDecoderPrimer.class.getClassLoader());
        }

        @Override
        protected Class<?> loadClass(String name, boolean resolve)
                throws ClassNotFoundException
        {
            if (!name.startsWith(ISOLATED_PACKAGE)) {
                return super.loadClass(name, resolve);
            }
            synchronized (getClassLoadingLock(name)) {
                Class<?> loaded = findLoadedClass(name);
                if (loaded == null) {
                    loaded = defineFromParentResource(name);
                }
                if (resolve) {
                    resolveClass(loaded);
                }
                return loaded;
            }
        }

        private Class<?> defineFromParentResource(String name)
                throws ClassNotFoundException
        {
            String resource = name.replace('.', '/') + ".class";
            try (InputStream in = getParent().getResourceAsStream(resource)) {
                if (in == null) {
                    throw new ClassNotFoundException(name);
                }
                byte[] bytes = in.readAllBytes();
                return defineClass(name, bytes, 0, bytes.length);
            }
            catch (IOException e) {
                throw new ClassNotFoundException(name, e);
            }
        }
    }
}
