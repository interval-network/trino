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
package io.trino.plugin.opa;

import com.google.common.annotations.VisibleForTesting;
import io.trino.plugin.opa.schema.OpaQueryInput;
import io.trino.spi.eventlistener.EventListener;
import io.trino.spi.eventlistener.QueryCompletedEvent;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;

/**
 * Remembers AccessCatalog decisions for the lifetime of one query.
 * <p>
 * Trino checks AccessCatalog before almost every table-level check, so one query asks OPA
 * the same AccessCatalog question many times: once per SelectFromColumns, including one per
 * masked column. Within a query this memo makes one OPA call per distinct request and replays
 * that decision for the identical requests that follow.
 * <p>
 * The key is the complete OPA input: query id, user, groups, catalog, software stack and
 * additional context. A replay can only answer a request whose body would be byte-identical
 * to the one OPA already decided. OPA errors are never stored, so the next call retries.
 * The plugin cannot see the policy bundle revision, so a bundle activated mid-query takes effect
 * for this query's AccessCatalog checks only on a request the memo has not seen yet.
 * <p>
 * The memo is freed when Trino reports the query completed. Two bounds keep a missed
 * completion event from leaking: at most {@value #MAX_TRACKED_QUERIES} queries are tracked,
 * and at most {@value #MAX_DECISIONS_PER_QUERY} decisions per query. When the query limit is
 * reached, memos for queries that started more than {@link #STALE_AFTER} ago are dropped. If
 * the limit is still reached after that, the request goes to OPA without being memoized.
 * Dropping a memo early costs OPA calls, never correctness. Both bounds are soft: concurrent
 * first checks can overshoot each one by at most the number of concurrent callers.
 * <p>
 * The OPA call for a new input runs inside the per-query map's computeIfAbsent, so a
 * concurrent identical check waits for it instead of making a second call. Only checks of
 * the same query can wait there; each query has its own map.
 */
final class OpaAccessCatalogMemo
        implements EventListener
{
    static final int MAX_TRACKED_QUERIES = 10_000;
    static final int MAX_DECISIONS_PER_QUERY = 1_000;
    static final Duration STALE_AFTER = Duration.ofHours(1);

    private final Map<String, QueryMemo> memos = new ConcurrentHashMap<>();
    private final Supplier<Instant> clock;

    OpaAccessCatalogMemo()
    {
        this(Instant::now);
    }

    @VisibleForTesting
    OpaAccessCatalogMemo(Supplier<Instant> clock)
    {
        this.clock = requireNonNull(clock, "clock is null");
    }

    boolean decide(String queryId, Instant queryStart, OpaQueryInput input, BooleanSupplier opaDecision)
    {
        QueryMemo memo = memoFor(queryId, queryStart);
        if (memo == null) {
            return opaDecision.getAsBoolean();
        }
        Boolean known = memo.decisions().get(input);
        if (known != null) {
            return known;
        }
        if (memo.decisions().size() >= MAX_DECISIONS_PER_QUERY) {
            return opaDecision.getAsBoolean();
        }
        // computeIfAbsent makes concurrent identical checks share one OPA call. If the call
        // throws, nothing is stored.
        return memo.decisions().computeIfAbsent(input, _ -> opaDecision.getAsBoolean());
    }

    @Override
    public void queryCompleted(QueryCompletedEvent queryCompletedEvent)
    {
        memos.remove(queryCompletedEvent.getMetadata().getQueryId());
    }

    @VisibleForTesting
    int trackedQueries()
    {
        return memos.size();
    }

    private QueryMemo memoFor(String queryId, Instant queryStart)
    {
        QueryMemo memo = memos.get(queryId);
        if (memo != null) {
            return memo;
        }
        if (memos.size() >= MAX_TRACKED_QUERIES) {
            Instant cutoff = clock.get().minus(STALE_AFTER);
            memos.values().removeIf(stale -> stale.queryStart().isBefore(cutoff));
            if (memos.size() >= MAX_TRACKED_QUERIES) {
                return null;
            }
        }
        return memos.computeIfAbsent(queryId, _ -> new QueryMemo(queryStart, new ConcurrentHashMap<>()));
    }

    private record QueryMemo(Instant queryStart, Map<OpaQueryInput, Boolean> decisions)
    {
        QueryMemo
        {
            requireNonNull(queryStart, "queryStart is null");
            requireNonNull(decisions, "decisions is null");
        }
    }
}
