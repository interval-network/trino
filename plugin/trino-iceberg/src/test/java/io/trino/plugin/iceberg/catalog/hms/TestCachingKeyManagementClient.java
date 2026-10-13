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

import com.github.benmanes.caffeine.cache.Ticker;
import io.airlift.units.Duration;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.encryption.KeyManagementClient;
import org.apache.iceberg.encryption.StandardEncryptionManager;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.NotSerializableException;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

final class TestCachingKeyManagementClient
{
    private static final String KEY_ID = "projects/p/locations/l/keyRings/k/cryptoKeys/table-key";

    private final FakeTicker ticker = new FakeTicker();

    @Test
    void testRepeatedUnwrapHitsKmsOnce()
    {
        CountingKms kms = new CountingKms();
        CachingKeyManagementClient client = cache(kms, 300);

        ByteBuffer first = client.unwrapKey(wrapped("dek-1"), KEY_ID);
        ByteBuffer second = client.unwrapKey(wrapped("dek-1"), KEY_ID);

        assertThat(kms.unwrapCalls()).isEqualTo(1);
        assertThat(first).isEqualTo(plain("dek-1"));
        assertThat(second).isEqualTo(plain("dek-1"));
    }

    @Test
    void testDistinctCiphertextsAndKeyIdsAreDistinctEntries()
    {
        CountingKms kms = new CountingKms();
        CachingKeyManagementClient client = cache(kms, 300);

        assertThat(client.unwrapKey(wrapped("dek-1"), KEY_ID)).isEqualTo(plain("dek-1"));
        assertThat(client.unwrapKey(wrapped("dek-2"), KEY_ID)).isEqualTo(plain("dek-2"));
        assertThat(client.unwrapKey(wrapped("dek-1"), KEY_ID + "-other")).isEqualTo(plain("dek-1"));

        assertThat(kms.unwrapCalls()).isEqualTo(3);
    }

    @Test
    void testSameCiphertextUnderAnotherKeyIdIsNeverServedFromCache()
    {
        CountingKms kms = new CountingKms();
        CachingKeyManagementClient client = cache(kms, 300);
        client.unwrapKey(wrapped("dek-1"), KEY_ID);

        // The caller is not authorised for this key; the cached result for KEY_ID must not answer for it
        assertThatThrownBy(() -> client.unwrapKey(wrapped("dek-1"), "projects/p/locations/l/keyRings/k/cryptoKeys/forbidden"))
                .hasMessageContaining("PERMISSION_DENIED");
    }

    @Test
    void testDisabledKeyTakesEffectWithinTtl()
    {
        CountingKms kms = new CountingKms();
        CachingKeyManagementClient client = cache(kms, 300);
        client.unwrapKey(wrapped("dek-1"), KEY_ID);

        // The KMS key is disabled. Inside the TTL the cached material is still served: this is the accepted window
        kms.disabled.set(true);
        ticker.advance(299);
        assertThat(client.unwrapKey(wrapped("dek-1"), KEY_ID)).isEqualTo(plain("dek-1"));

        // Expiry is fixed from the write, not extended by the read above, so the key stops working at the TTL
        ticker.advance(2);
        assertThatThrownBy(() -> client.unwrapKey(wrapped("dek-1"), KEY_ID))
                .hasMessageContaining("key is disabled");
        // and a failure is never cached: it keeps failing, and recovers only when KMS does
        assertThatThrownBy(() -> client.unwrapKey(wrapped("dek-1"), KEY_ID))
                .hasMessageContaining("key is disabled");
        kms.disabled.set(false);
        assertThat(client.unwrapKey(wrapped("dek-1"), KEY_ID)).isEqualTo(plain("dek-1"));
    }

    @Test
    void testEvictedMaterialIsZeroed()
    {
        CountingKms kms = new CountingKms();
        CachingKeyManagementClient client = cache(kms, 300);
        client.unwrapKey(wrapped("dek-1"), KEY_ID);
        // The cache stores its own copy, not the delegate's array, and that copy is what eviction must zero
        byte[] stored = client.cachedValueForTesting(wrapped("dek-1"), KEY_ID);
        assertThat(stored).isEqualTo(plain("dek-1").array());
        assertThat(stored).isNotSameAs(kms.lastReturned());

        ticker.advance(301);
        client.cleanUp();

        assertThat(stored).containsOnly((byte) 0);
    }

    @Test
    void testInvalidateAndCloseZeroEverything()
    {
        CountingKms kms = new CountingKms();
        CachingKeyManagementClient client = cache(kms, 300);
        client.unwrapKey(wrapped("dek-1"), KEY_ID);
        byte[] stored = client.cachedValueForTesting(wrapped("dek-1"), KEY_ID);

        client.close();

        assertThat(stored).containsOnly((byte) 0);
        assertThat(client.size()).isZero();
        assertThat(kms.closeCalls.get()).isEqualTo(1);
    }

    @Test
    void testConcurrentMissesShareOneKmsCall()
            throws Exception
    {
        CountDownLatch release = new CountDownLatch(1);
        CountingKms kms = new CountingKms();
        kms.gate = release;
        CachingKeyManagementClient client = cache(kms, 300);
        ExecutorService executor = Executors.newFixedThreadPool(8);
        try {
            List<Future<ByteBuffer>> results = new ArrayList<>();
            for (int i = 0; i < 8; i++) {
                results.add(executor.submit(() -> client.unwrapKey(wrapped("dek-1"), KEY_ID)));
            }
            // let every thread reach the cache before the one KMS call returns
            Thread.sleep(200);
            release.countDown();
            for (Future<ByteBuffer> result : results) {
                assertThat(result.get(10, SECONDS)).isEqualTo(plain("dek-1"));
            }
        }
        finally {
            executor.shutdownNow();
        }
        assertThat(kms.unwrapCalls()).isEqualTo(1);
    }

    @Test
    void testDelegateReceivesAWritableBuffer()
    {
        // A pluggable client (OpenBao) may call array() on its input, which a read-only buffer rejects
        CountingKms kms = new CountingKms();
        kms.requireArray = true;
        CachingKeyManagementClient client = cache(kms, 300);

        assertThat(client.unwrapKey(wrapped("dek-1").asReadOnlyBuffer(), KEY_ID)).isEqualTo(plain("dek-1"));
    }

    @Test
    void testUnwrapAfterCloseIsNotCached()
    {
        CountingKms kms = new CountingKms();
        CachingKeyManagementClient client = cache(kms, 300);
        client.close();

        client.unwrapKey(wrapped("dek-1"), KEY_ID);
        client.unwrapKey(wrapped("dek-1"), KEY_ID);

        assertThat(kms.unwrapCalls()).isEqualTo(2);
        assertThat(client.size()).isZero();
    }

    @Test
    void testRefusesSerialization()
    {
        CachingKeyManagementClient client = cache(new CountingKms(), 300);

        assertThatThrownBy(() -> new ObjectOutputStream(new ByteArrayOutputStream()).writeObject(client))
                .isInstanceOf(NotSerializableException.class);
    }

    @Test
    void testSizeIsBounded()
    {
        CountingKms kms = new CountingKms();
        CachingKeyManagementClient client = new CachingKeyManagementClient(kms, new Duration(300, SECONDS), 2, ticker);
        for (int i = 0; i < 10; i++) {
            client.unwrapKey(wrapped("dek-" + i), KEY_ID);
        }

        assertThat(client.size()).isLessThanOrEqualTo(2);
    }

    @Test
    void testCallersGetIndependentCopiesAndKeepTheirBufferPosition()
    {
        CountingKms kms = new CountingKms();
        CachingKeyManagementClient client = cache(kms, 300);

        ByteBuffer input = wrapped("dek-1");
        ByteBuffer first = client.unwrapKey(input, KEY_ID);
        assertThat(input.remaining()).isEqualTo(wrapped("dek-1").remaining());

        // A caller that scribbles over its result must not corrupt what the next caller receives
        first.put(0, (byte) 0x7F);
        assertThat(client.unwrapKey(wrapped("dek-1"), KEY_ID)).isEqualTo(plain("dek-1"));
    }

    @Test
    void testWrapAndKeyGenerationPassThrough()
    {
        CountingKms kms = new CountingKms();
        CachingKeyManagementClient client = cache(kms, 300);

        client.wrapKey(plain("dek-1"), KEY_ID);
        client.wrapKey(plain("dek-1"), KEY_ID);

        assertThat(kms.wrapCalls.get()).isEqualTo(2);
        assertThat(client.supportsKeyGeneration()).isTrue();
        assertThat(client.generateKey(KEY_ID).key()).isEqualTo(plain("generated"));
    }

    @Test
    void testEntryToStringCarriesNoKeyMaterial()
    {
        CountingKms kms = new CountingKms();
        CachingKeyManagementClient client = cache(kms, 300);
        client.unwrapKey(wrapped("dek-secret"), KEY_ID);

        assertThat(client.entriesForTesting())
                .singleElement()
                .asString()
                .contains(KEY_ID)
                .doesNotContain("dek-secret")
                .doesNotContain(new String(wrapped("dek-secret").array(), UTF_8));
    }

    @Test
    void testRejectsNonPositiveTtl()
    {
        assertThatThrownBy(() -> new CachingKeyManagementClient(new CountingKms(), new Duration(0, SECONDS), 10))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testPerQueryEncryptionManagersShareOneKmsUnwrap()
    {
        // Each query builds its own StandardEncryptionManager, whose own memo dies with the query. The shared
        // caching client is what lets the second "query" skip KMS.
        CountingKms kms = new CountingKms();
        StandardEncryptionManagerFactory factory = new StandardEncryptionManagerFactory(cache(kms, 300));
        TableMetadata metadata = encryptedTableMetadata();

        EncryptionManager firstQuery = factory.create(metadata).orElseThrow();
        EncryptionManager secondQuery = factory.create(metadata).orElseThrow();
        assertThat(((StandardEncryptionManager) firstQuery).unwrapKey(wrapped("dek-1"))).isEqualTo(plain("dek-1"));
        assertThat(((StandardEncryptionManager) secondQuery).unwrapKey(wrapped("dek-1"))).isEqualTo(plain("dek-1"));

        assertThat(kms.unwrapCalls()).isEqualTo(1);
    }

    @Test
    void testWithoutCachePerQueryManagersEachCallKms()
    {
        CountingKms kms = new CountingKms();
        StandardEncryptionManagerFactory factory = new StandardEncryptionManagerFactory(kms);
        TableMetadata metadata = encryptedTableMetadata();

        ((StandardEncryptionManager) factory.create(metadata).orElseThrow()).unwrapKey(wrapped("dek-1"));
        ((StandardEncryptionManager) factory.create(metadata).orElseThrow()).unwrapKey(wrapped("dek-1"));

        assertThat(kms.unwrapCalls()).isEqualTo(2);
    }

    private CachingKeyManagementClient cache(KeyManagementClient kms, int ttlSeconds)
    {
        return new CachingKeyManagementClient(kms, new Duration(ttlSeconds, SECONDS), 100, ticker);
    }

    private static TableMetadata encryptedTableMetadata()
    {
        return TableMetadata.buildFromEmpty(3)
                .assignUUID()
                .setLocation("/test/location")
                .setCurrentSchema(new Schema(Types.NestedField.required(1, "id", Types.LongType.get())), 1)
                .addPartitionSpec(PartitionSpec.unpartitioned())
                .addSortOrder(SortOrder.unsorted())
                .setProperties(Map.of("encryption.key-id", KEY_ID))
                .build();
    }

    // The fake KMS "wraps" by reversing bytes, so a wrapped and a plain key are distinguishable
    private static ByteBuffer wrapped(String plain)
    {
        byte[] bytes = plain.getBytes(UTF_8);
        byte[] reversed = new byte[bytes.length];
        for (int i = 0; i < bytes.length; i++) {
            reversed[i] = bytes[bytes.length - 1 - i];
        }
        return ByteBuffer.wrap(reversed);
    }

    private static ByteBuffer plain(String plain)
    {
        return ByteBuffer.wrap(plain.getBytes(UTF_8));
    }

    private static final class CountingKms
            implements KeyManagementClient
    {
        private final AtomicInteger unwraps = new AtomicInteger();
        private final AtomicInteger wrapCalls = new AtomicInteger();
        private final AtomicInteger closeCalls = new AtomicInteger();
        private final AtomicBoolean disabled = new AtomicBoolean();
        private volatile byte[] lastReturned;
        private volatile CountDownLatch gate;
        private volatile boolean requireArray;

        int unwrapCalls()
        {
            return unwraps.get();
        }

        byte[] lastReturned()
        {
            return lastReturned;
        }

        @Override
        public ByteBuffer wrapKey(ByteBuffer key, String wrappingKeyId)
        {
            wrapCalls.incrementAndGet();
            return key;
        }

        @Override
        public ByteBuffer unwrapKey(ByteBuffer wrappedKey, String wrappingKeyId)
        {
            unwraps.incrementAndGet();
            if (gate != null) {
                try {
                    gate.await();
                }
                catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(e);
                }
            }
            if (disabled.get()) {
                throw new IllegalStateException("FAILED_PRECONDITION: key is disabled");
            }
            if (wrappingKeyId.endsWith("/forbidden")) {
                throw new IllegalStateException("PERMISSION_DENIED: " + wrappingKeyId);
            }
            byte[] bytes = new byte[wrappedKey.remaining()];
            if (requireArray) {
                // Read through the backing array, pairing array() with arrayOffset() as error-prone's
                // ByteBufferBackingArray check requires. This keeps the point of the check: a read-only or
                // non-array-backed buffer throws here, exactly what an array()-calling pluggable KMS
                // (e.g. OpenBao) would hit if the client handed it the caller's buffer.
                System.arraycopy(wrappedKey.array(), wrappedKey.arrayOffset() + wrappedKey.position(), bytes, 0, bytes.length);
            }
            else {
                wrappedKey.duplicate().get(bytes);
            }
            byte[] reversed = new byte[bytes.length];
            for (int i = 0; i < bytes.length; i++) {
                reversed[i] = bytes[bytes.length - 1 - i];
            }
            lastReturned = reversed;
            return ByteBuffer.wrap(reversed);
        }

        @Override
        public boolean supportsKeyGeneration()
        {
            return true;
        }

        @Override
        public KeyGenerationResult generateKey(String wrappingKeyId)
        {
            return new KeyGenerationResult(plain("generated"), wrapped("generated"));
        }

        @Override
        public void initialize(Map<String, String> properties) {}

        @Override
        public void close()
        {
            closeCalls.incrementAndGet();
        }
    }

    private static final class FakeTicker
            implements Ticker
    {
        private final AtomicLong nanos = new AtomicLong();

        void advance(long seconds)
        {
            nanos.addAndGet(SECONDS.toNanos(seconds));
        }

        @Override
        public long read()
        {
            return nanos.get();
        }
    }
}
