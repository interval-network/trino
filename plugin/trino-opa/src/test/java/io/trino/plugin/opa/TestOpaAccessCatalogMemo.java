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

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.trino.Session;
import io.trino.execution.QueryIdGenerator;
import io.trino.plugin.blackhole.BlackHolePlugin;
import io.trino.plugin.opa.HttpClientUtils.InstrumentedHttpClient;
import io.trino.plugin.opa.HttpClientUtils.MockResponse;
import io.trino.plugin.opa.schema.OpaPluginContext;
import io.trino.plugin.opa.schema.OpaQueryContext;
import io.trino.plugin.opa.schema.OpaQueryInput;
import io.trino.plugin.opa.schema.OpaQueryInputAction;
import io.trino.plugin.opa.schema.OpaQueryInputResource;
import io.trino.plugin.opa.schema.TrinoIdentity;
import io.trino.spi.QueryId;
import io.trino.spi.security.Identity;
import io.trino.spi.security.SystemSecurityContext;
import io.trino.testing.StandaloneQueryRunner;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.plugin.opa.OpaAccessCatalogMemo.MAX_DECISIONS_PER_QUERY;
import static io.trino.plugin.opa.OpaAccessCatalogMemo.MAX_TRACKED_QUERIES;
import static io.trino.plugin.opa.OpaAccessCatalogMemo.STALE_AFTER;
import static io.trino.plugin.opa.TestConstants.NO_ACCESS_RESPONSE;
import static io.trino.plugin.opa.TestConstants.OK_RESPONSE;
import static io.trino.plugin.opa.TestConstants.OPA_SERVER_URI;
import static io.trino.plugin.opa.TestConstants.SERVER_ERROR_RESPONSE;
import static io.trino.plugin.opa.TestConstants.batchFilteringOpaConfig;
import static io.trino.plugin.opa.TestConstants.simpleOpaConfig;
import static io.trino.plugin.opa.TestHelpers.createMockHttpClient;
import static io.trino.plugin.opa.TestHelpers.createOpaAuthorizer;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.util.concurrent.Executors.newFixedThreadPool;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

final class TestOpaAccessCatalogMemo
{
    private static final Identity ALICE = Identity.forUser("alice").withGroups(ImmutableSet.of("analysts")).build();
    private static final Identity ALICE_OTHER_GROUPS = Identity.forUser("alice").withGroups(ImmutableSet.of("admins")).build();
    private static final Identity BOB = Identity.forUser("bob").withGroups(ImmutableSet.of("analysts")).build();
    private static final QueryIdGenerator QUERY_IDS = new QueryIdGenerator();

    @Test
    void testRepeatedCheckInOneQueryCallsOpaOnce()
    {
        InstrumentedHttpClient client = createMockHttpClient(OPA_SERVER_URI, _ -> OK_RESPONSE);
        OpaAccessControl accessControl = createOpaAuthorizer(simpleOpaConfig(), client);
        SystemSecurityContext context = context(ALICE, QUERY_IDS.createNextQueryId());

        for (int i = 0; i < 5; i++) {
            assertThat(accessControl.canAccessCatalog(context, "analytics")).isTrue();
        }
        assertThat(accessCatalogRequests(client)).hasSize(1);
    }

    @Test
    void testConcurrentIdenticalChecksShareOneCall()
            throws Exception
    {
        CountDownLatch release = new CountDownLatch(1);
        InstrumentedHttpClient client = createMockHttpClient(OPA_SERVER_URI, _ -> {
            try {
                release.await(30, SECONDS);
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
            return OK_RESPONSE;
        });
        OpaAccessControl accessControl = createOpaAuthorizer(simpleOpaConfig(), client);
        SystemSecurityContext context = context(ALICE, QUERY_IDS.createNextQueryId());

        ExecutorService executor = newFixedThreadPool(8);
        try {
            List<Future<Boolean>> results = new ArrayList<>();
            for (int i = 0; i < 8; i++) {
                results.add(executor.submit(() -> accessControl.canAccessCatalog(context, "analytics")));
            }
            Thread.sleep(200);
            release.countDown();
            for (Future<Boolean> result : results) {
                assertThat(result.get(30, SECONDS)).isTrue();
            }
        }
        finally {
            executor.shutdownNow();
        }
        assertThat(accessCatalogRequests(client)).hasSize(1);
    }

    @Test
    void testDenialIsReplayedUnchanged()
    {
        InstrumentedHttpClient client = createMockHttpClient(OPA_SERVER_URI, _ -> NO_ACCESS_RESPONSE);
        OpaAccessControl accessControl = createOpaAuthorizer(simpleOpaConfig(), client);
        SystemSecurityContext context = context(ALICE, QUERY_IDS.createNextQueryId());

        assertThat(accessControl.canAccessCatalog(context, "analytics")).isFalse();
        assertThat(accessControl.canAccessCatalog(context, "analytics")).isFalse();
        assertThat(accessCatalogRequests(client)).hasSize(1);
    }

    @Test
    void testBatchAuthorizerMemoizesToo()
    {
        InstrumentedHttpClient client = createMockHttpClient(OPA_SERVER_URI, _ -> OK_RESPONSE);
        OpaAccessControl accessControl = createOpaAuthorizer(batchFilteringOpaConfig(), client);
        assertThat(accessControl).isInstanceOf(OpaBatchAccessControl.class);
        SystemSecurityContext context = context(ALICE, QUERY_IDS.createNextQueryId());

        accessControl.canAccessCatalog(context, "analytics");
        accessControl.canAccessCatalog(context, "analytics");
        assertThat(accessCatalogRequests(client)).hasSize(1);
    }

    @Test
    void testEveryKeyPartSeparatesDecisions()
    {
        // Answers by catalog and user, so a memo hit on the wrong key would return the wrong answer.
        InstrumentedHttpClient client = createMockHttpClient(OPA_SERVER_URI, request ->
                request.at("/input/action/resource/catalog/name").asText().equals("analytics")
                        && request.at("/input/context/identity/user").asText().equals("alice")
                        && request.at("/input/context/identity/groups/0").asText().equals("analysts")
                        ? OK_RESPONSE
                        : NO_ACCESS_RESPONSE);
        OpaAccessControl accessControl = createOpaAuthorizer(simpleOpaConfig(), client);
        QueryId query = QUERY_IDS.createNextQueryId();

        assertThat(accessControl.canAccessCatalog(context(ALICE, query), "analytics")).isTrue();
        assertThat(accessControl.canAccessCatalog(context(ALICE, query), "audit")).isFalse();
        assertThat(accessControl.canAccessCatalog(context(BOB, query), "analytics")).isFalse();
        assertThat(accessControl.canAccessCatalog(context(ALICE_OTHER_GROUPS, query), "analytics")).isFalse();
        assertThat(accessControl.canAccessCatalog(context(ALICE, QUERY_IDS.createNextQueryId()), "analytics")).isTrue();
        assertThat(accessCatalogRequests(client)).hasSize(5);

        // Each of the five is now memoized under its own key.
        assertThat(accessControl.canAccessCatalog(context(ALICE, query), "analytics")).isTrue();
        assertThat(accessControl.canAccessCatalog(context(ALICE, query), "audit")).isFalse();
        assertThat(accessControl.canAccessCatalog(context(BOB, query), "analytics")).isFalse();
        assertThat(accessControl.canAccessCatalog(context(ALICE_OTHER_GROUPS, query), "analytics")).isFalse();
        assertThat(accessCatalogRequests(client)).hasSize(5);
    }

    @Test
    void testOpaErrorIsNotMemoized()
    {
        AtomicReference<MockResponse> response = new AtomicReference<>(SERVER_ERROR_RESPONSE);
        InstrumentedHttpClient client = createMockHttpClient(OPA_SERVER_URI, _ -> response.get());
        OpaAccessControl accessControl = createOpaAuthorizer(simpleOpaConfig(), client);
        SystemSecurityContext context = context(ALICE, QUERY_IDS.createNextQueryId());

        assertThatThrownBy(() -> accessControl.canAccessCatalog(context, "analytics"))
                .isInstanceOf(OpaQueryException.class);
        response.set(OK_RESPONSE);
        assertThat(accessControl.canAccessCatalog(context, "analytics")).isTrue();
        assertThat(accessControl.canAccessCatalog(context, "analytics")).isTrue();
        assertThat(accessCatalogRequests(client)).hasSize(2);
    }

    @Test
    void testOnlyAccessCatalogIsMemoized()
    {
        InstrumentedHttpClient client = createMockHttpClient(OPA_SERVER_URI, _ -> OK_RESPONSE);
        OpaAccessControl accessControl = createOpaAuthorizer(simpleOpaConfig(), client);
        SystemSecurityContext context = context(ALICE, QUERY_IDS.createNextQueryId());

        accessControl.checkCanShowSchemas(context, "analytics");
        accessControl.checkCanShowSchemas(context, "analytics");
        assertThat(client.getRequests()).hasSize(2);
    }

    @Test
    void testEventListenerIsTheMemo()
    {
        OpaAccessControl accessControl = createOpaAuthorizer(simpleOpaConfig(), createMockHttpClient(OPA_SERVER_URI, _ -> OK_RESPONSE));
        assertThat(getOnlyElement(accessControl.getEventListeners())).isInstanceOf(OpaAccessCatalogMemo.class);
    }

    @Test
    void testTrackedQueriesAreBounded()
    {
        AtomicReference<Instant> now = new AtomicReference<>(Instant.parse("2026-10-11T00:00:00Z"));
        OpaAccessCatalogMemo memo = new OpaAccessCatalogMemo(now::get);
        AtomicInteger opaCalls = new AtomicInteger();
        Instant start = now.get();
        for (int i = 0; i < MAX_TRACKED_QUERIES; i++) {
            memo.decide("query_" + i, start, input("query_" + i, "analytics"), () -> opaCalls.incrementAndGet() > 0);
        }
        assertThat(memo.trackedQueries()).isEqualTo(MAX_TRACKED_QUERIES);

        // Full, nothing stale: a new query goes straight to OPA every time and is not tracked.
        memo.decide("overflow", start, input("overflow", "analytics"), () -> opaCalls.incrementAndGet() > 0);
        memo.decide("overflow", start, input("overflow", "analytics"), () -> opaCalls.incrementAndGet() > 0);
        assertThat(opaCalls.get()).isEqualTo(MAX_TRACKED_QUERIES + 2);
        assertThat(memo.trackedQueries()).isEqualTo(MAX_TRACKED_QUERIES);

        // Already-tracked queries still hit their memo.
        memo.decide("query_0", start, input("query_0", "analytics"), () -> opaCalls.incrementAndGet() > 0);
        assertThat(opaCalls.get()).isEqualTo(MAX_TRACKED_QUERIES + 2);

        // Once the tracked queries are older than STALE_AFTER they are dropped to make room.
        now.set(start.plus(STALE_AFTER).plus(Duration.ofSeconds(1)));
        memo.decide("fresh", now.get(), input("fresh", "analytics"), () -> opaCalls.incrementAndGet() > 0);
        memo.decide("fresh", now.get(), input("fresh", "analytics"), () -> opaCalls.incrementAndGet() > 0);
        assertThat(opaCalls.get()).isEqualTo(MAX_TRACKED_QUERIES + 3);
        assertThat(memo.trackedQueries()).isEqualTo(1);
    }

    @Test
    void testDecisionsPerQueryAreBounded()
    {
        OpaAccessCatalogMemo memo = new OpaAccessCatalogMemo();
        AtomicInteger opaCalls = new AtomicInteger();
        Instant start = Instant.now();
        for (int i = 0; i < MAX_DECISIONS_PER_QUERY; i++) {
            memo.decide("q", start, input("q", "catalog_" + i), () -> opaCalls.incrementAndGet() > 0);
        }
        memo.decide("q", start, input("q", "one_too_many"), () -> opaCalls.incrementAndGet() > 0);
        memo.decide("q", start, input("q", "one_too_many"), () -> opaCalls.incrementAndGet() > 0);
        assertThat(opaCalls.get()).isEqualTo(MAX_DECISIONS_PER_QUERY + 2);

        // Decisions stored before the bound are still replayed.
        memo.decide("q", start, input("q", "catalog_0"), () -> opaCalls.incrementAndGet() > 0);
        assertThat(opaCalls.get()).isEqualTo(MAX_DECISIONS_PER_QUERY + 2);
    }

    /**
     * End to end through Trino's analyzer: a join of two tables checks SelectFromColumns twice, and
     * Trino checks AccessCatalog before each one. The memo turns those into one OPA call per
     * query, and Trino's query-completed event frees the memo.
     */
    @Test
    void testQueryLifecycle()
            throws Exception
    {
        InstrumentedHttpClient client = createMockHttpClient(OPA_SERVER_URI, _ -> OK_RESPONSE);
        OpaAccessControl accessControl = createOpaAuthorizer(simpleOpaConfig(), client);
        OpaAccessCatalogMemo memo = (OpaAccessCatalogMemo) getOnlyElement(accessControl.getEventListeners());
        Session session = testSessionBuilder().setIdentity(ALICE).build();

        try (StandaloneQueryRunner runner = new StandaloneQueryRunner(session, builder -> builder
                .setSystemAccessControl(accessControl)
                .setEventListeners(ImmutableList.copyOf(accessControl.getEventListeners())))) {
            runner.installPlugin(new BlackHolePlugin());
            runner.createCatalog("catalog_one", "blackhole");
            runner.execute(session, "CREATE SCHEMA catalog_one.s");
            runner.execute(session, "CREATE TABLE catalog_one.s.t (a bigint, b bigint)");
            runner.execute(session, "CREATE TABLE catalog_one.s.u (a bigint, b bigint)");
            awaitQueriesCompleted(memo);

            int before = client.getRequests().size();
            runner.execute(session, "SELECT t.a, u.b FROM catalog_one.s.t JOIN catalog_one.s.u ON t.a = u.a");
            List<JsonNode> queryRequests = client.getRequests().subList(before, client.getRequests().size());

            List<String> catalogChecks = queryRequests.stream()
                    .filter(request -> request.at("/input/action/operation").asText().equals("AccessCatalog"))
                    .map(request -> request.at("/input/context/queryId").asText() + "/" + request.at("/input/action/resource/catalog/name").asText())
                    .collect(toImmutableList());
            assertThat(catalogChecks).hasSize(1);
            assertThat(queryRequests.stream().filter(request -> request.at("/input/action/operation").asText().equals("SelectFromColumns")))
                    .hasSize(2);

            awaitQueriesCompleted(memo);
        }
    }

    // Trino fires the query-completed event after the client has its result.
    private static void awaitQueriesCompleted(OpaAccessCatalogMemo memo)
            throws InterruptedException
    {
        long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
        while (memo.trackedQueries() != 0 && System.nanoTime() < deadline) {
            Thread.sleep(10);
        }
        assertThat(memo.trackedQueries()).isZero();
    }

    private static List<JsonNode> accessCatalogRequests(InstrumentedHttpClient client)
    {
        return client.getRequests().stream()
                .filter(request -> request.at("/input/action/operation").asText().equals("AccessCatalog"))
                .collect(toImmutableList());
    }

    private static SystemSecurityContext context(Identity identity, QueryId queryId)
    {
        return new SystemSecurityContext(identity, queryId, Instant.now());
    }

    private static OpaQueryInput input(String queryId, String catalog)
    {
        return new OpaQueryInput(
                new OpaQueryContext(new TrinoIdentity("alice", ImmutableSet.of()), new OpaPluginContext("483"), ImmutableMap.of(), Optional.of(QueryId.valueOf(queryId))),
                OpaQueryInputAction.builder().operation("AccessCatalog").resource(OpaQueryInputResource.builder().catalog(catalog).build()).build());
    }
}
