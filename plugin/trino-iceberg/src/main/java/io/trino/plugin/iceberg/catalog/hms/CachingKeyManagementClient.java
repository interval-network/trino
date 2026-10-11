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

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalCause;
import com.github.benmanes.caffeine.cache.Scheduler;
import com.github.benmanes.caffeine.cache.Ticker;
import com.google.common.annotations.VisibleForTesting;
import io.airlift.units.Duration;
import org.apache.iceberg.encryption.KeyManagementClient;

import java.io.NotSerializableException;
import java.io.ObjectOutputStream;
import java.io.Serial;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

/// Interval fork (PLE-552): a bounded, short-TTL cache of KMS-unwrapped key material in front of the catalog's
/// shared [KeyManagementClient].
///
/// Iceberg's `StandardEncryptionManager` already memoises unwrapped keys, but only per manager, and a manager
/// is built per loaded table, so per query. Every query on an encrypted (PARE) table therefore makes at least
/// one network KMS `decrypt` from an `iceberg-split-source` thread before it can read the manifest list. A
/// SOV-148 coordinator dump on 2026-10-10 had 31 split-source threads parked in that call. This cache lets the
/// queries on one node share the result for a bounded time.
///
/// **Key.** The wrapping key id together with the exact wrapped bytes. Two different ciphertexts never share an
/// entry, and the same ciphertext under a different key id never does either. The key holds only ciphertext,
/// which is already stored in table metadata and manifests and is not secret.
///
/// **Security trade-off (sign-off required beyond devprod).** Unwrapped key material normally lives in memory
/// only for the life of a query; with this cache it lives up to `encryption.kms.key-cache-ttl` longer. Entries
/// expire a fixed time after they were written (never refreshed by reads), so a KMS key disabled, destroyed, or
/// access-revoked in KMS stops being usable on this node within that TTL, as do key versions removed by
/// rotation. Entries evicted for any reason (expiry, size, invalidation, close) are overwritten with zeros
/// first. That is best effort: the JVM may already have copied the array during garbage collection, and each
/// caller receives its own copy, which this class cannot zero. Key material is never logged; nothing here
/// logs at all, and the key's `toString` prints only the key id and the ciphertext length.
///
/// Errors are never cached: a failed unwrap is retried by the next caller, so a revoked key keeps failing.
/// Concurrent misses for the same key share one KMS call. The instance refuses Java serialization.
public final class CachingKeyManagementClient
        implements KeyManagementClient
{
    private final KeyManagementClient delegate;
    // transient: KeyManagementClient is Serializable, but a cache of plaintext keys must never be serialized
    private final transient Cache<WrappedKey, KeyMaterial> cache;
    private final transient AtomicLong unwrapCalls = new AtomicLong();
    private transient volatile boolean closed;

    public CachingKeyManagementClient(KeyManagementClient delegate, Duration ttl, long maxSize)
    {
        this(delegate, ttl, maxSize, Ticker.systemTicker());
    }

    @VisibleForTesting
    CachingKeyManagementClient(KeyManagementClient delegate, Duration ttl, long maxSize, Ticker ticker)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
        requireNonNull(ttl, "ttl is null");
        if (ttl.roundTo(NANOSECONDS) <= 0) {
            throw new IllegalArgumentException("key cache TTL must be positive: " + ttl);
        }
        this.cache = Caffeine.newBuilder()
                .expireAfterWrite(ttl.roundTo(NANOSECONDS), NANOSECONDS)
                .maximumSize(maxSize)
                .ticker(ticker)
                // Run on the evicting thread, so zeroing happens before the entry is gone rather than on a pool later
                .executor(Runnable::run)
                // Expire on a timer too: Caffeine otherwise expires lazily, on the next cache access, so on an idle
                // node plaintext could outlive the TTL
                .scheduler(Scheduler.systemScheduler())
                .removalListener((WrappedKey _, KeyMaterial value, RemovalCause _) -> {
                    if (value != null) {
                        value.destroy();
                    }
                })
                .build();
    }

    @Override
    public ByteBuffer unwrapKey(ByteBuffer wrappedKey, String wrappingKeyId)
    {
        requireNonNull(wrappedKey, "wrappedKey is null");
        requireNonNull(wrappingKeyId, "wrappingKeyId is null");
        WrappedKey key = new WrappedKey(wrappingKeyId, toBytes(wrappedKey));
        if (!closed) {
            KeyMaterial material = cache.get(key, k -> {
                unwrapCalls.incrementAndGet();
                return new KeyMaterial(toBytes(delegate.unwrapKey(ByteBuffer.wrap(k.ciphertext().clone()), k.keyId())));
            });
            // A copy per caller: the cached array must not escape, so callers cannot alter it and eviction can zero it.
            // Null when eviction zeroed it between the lookup and here; then fall through to an uncached unwrap.
            byte[] copy = material.copy();
            if (copy != null) {
                return ByteBuffer.wrap(copy);
            }
        }
        unwrapCalls.incrementAndGet();
        return delegate.unwrapKey(ByteBuffer.wrap(key.ciphertext().clone()), wrappingKeyId);
    }

    @Override
    public ByteBuffer wrapKey(ByteBuffer key, String wrappingKeyId)
    {
        return delegate.wrapKey(key, wrappingKeyId);
    }

    @Override
    public boolean supportsKeyGeneration()
    {
        return delegate.supportsKeyGeneration();
    }

    @Override
    public KeyGenerationResult generateKey(String wrappingKeyId)
    {
        return delegate.generateKey(wrappingKeyId);
    }

    @Override
    public void initialize(Map<String, String> properties)
    {
        delegate.initialize(properties);
    }

    @Override
    public void close()
    {
        closed = true;
        invalidateAll();
        delegate.close();
    }

    public void invalidateAll()
    {
        cache.invalidateAll();
        cache.cleanUp();
    }

    @VisibleForTesting
    long delegateUnwrapCount()
    {
        return unwrapCalls.get();
    }

    @VisibleForTesting
    long size()
    {
        cache.cleanUp();
        return cache.estimatedSize();
    }

    @VisibleForTesting
    void cleanUp()
    {
        cache.cleanUp();
    }

    // The cached array itself, so a test can observe that eviction zeroes it
    @VisibleForTesting
    byte[] cachedValueForTesting(ByteBuffer wrappedKey, String wrappingKeyId)
    {
        KeyMaterial material = cache.getIfPresent(new WrappedKey(wrappingKeyId, toBytes(wrappedKey)));
        return material == null ? null : material.bytes;
    }

    @VisibleForTesting
    List<Object> entriesForTesting()
    {
        return List.copyOf(cache.asMap().keySet());
    }

    private static byte[] toBytes(ByteBuffer buffer)
    {
        // Never consume the caller's buffer position
        ByteBuffer duplicate = buffer.duplicate();
        byte[] bytes = new byte[duplicate.remaining()];
        duplicate.get(bytes);
        return bytes;
    }

    // Plaintext must never be written out: deserializing would also leave the transient cache null
    @Serial
    private void writeObject(ObjectOutputStream out)
            throws NotSerializableException
    {
        throw new NotSerializableException(CachingKeyManagementClient.class.getName());
    }

    // Copy and destroy are serialized on the holder, so a caller never copies a half-zeroed key
    private static final class KeyMaterial
    {
        private final byte[] bytes;
        private boolean destroyed;

        private KeyMaterial(byte[] bytes)
        {
            this.bytes = bytes;
        }

        synchronized byte[] copy()
        {
            return destroyed ? null : bytes.clone();
        }

        synchronized void destroy()
        {
            Arrays.fill(bytes, (byte) 0);
            destroyed = true;
        }
    }

    private record WrappedKey(String keyId, byte[] ciphertext)
    {
        @Override
        public boolean equals(Object o)
        {
            return o instanceof WrappedKey other
                    && keyId.equals(other.keyId)
                    && Arrays.equals(ciphertext, other.ciphertext);
        }

        @Override
        public int hashCode()
        {
            return 31 * keyId.hashCode() + Arrays.hashCode(ciphertext);
        }

        @Override
        public String toString()
        {
            return "WrappedKey[keyId=" + keyId + ", ciphertextLength=" + ciphertext.length + "]";
        }
    }
}
