/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.geaflow.ai.operator;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.geaflow.ai.graph.GraphEntity;
import org.apache.geaflow.ai.graph.GraphVertex;
import org.apache.geaflow.ai.graph.LocalMemoryGraphAccessor;
import org.apache.geaflow.ai.graph.io.Edge;
import org.apache.geaflow.ai.graph.io.EdgeSchema;
import org.apache.geaflow.ai.graph.io.EntityGroup;
import org.apache.geaflow.ai.graph.io.GraphSchema;
import org.apache.geaflow.ai.graph.io.MemoryGraph;
import org.apache.geaflow.ai.graph.io.Vertex;
import org.apache.geaflow.ai.graph.io.VertexGroup;
import org.apache.geaflow.ai.graph.io.VertexSchema;
import org.apache.geaflow.ai.index.EntityAttributeIndexStore;
import org.apache.geaflow.ai.verbalization.SubgraphSemanticPromptFunction;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Verifies that reusing a resident keyword index is behaviourally equivalent to rebuilding a
 * throw-away index per query, and measures the difference.
 *
 * <p>Result comparison is done on sets rather than lists on purpose: the legacy path feeds Lucene
 * from a {@link HashMap} iteration, so its document order, and therefore its tie break order, was
 * never deterministic to begin with. For queries whose match count stays under the Lucene top N
 * limit the returned sets must be identical.
 */
public class ResidentSearchIndexTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(ResidentSearchIndexTest.class);

    private static final String LABEL = "doc";
    private static final String EDGE_LABEL = "rel";
    private static final int VERTEX_NUM = 10000;
    /** 10000 / 1000 = 10 matches per group token, safely below the Lucene top N of 30. */
    private static final int GROUP_NUM = 1000;
    private static final int QUERY_NUM = 10;

    private LocalMemoryGraphAccessor buildGraph(int vertexNum) {
        GraphSchema schema = new GraphSchema();
        VertexSchema vertexSchema = new VertexSchema(LABEL, "id", Collections.singletonList("text"));
        schema.setName("perf_graph");
        schema.addVertex(vertexSchema);

        List<Vertex> vertices = new ArrayList<>(vertexNum);
        for (int i = 0; i < vertexNum; i++) {
            vertices.add(newVertex(i));
        }
        Map<String, EntityGroup> entities = new HashMap<>();
        entities.put(LABEL, new VertexGroup(vertexSchema, vertices));
        return new LocalMemoryGraphAccessor(new MemoryGraph(schema, entities));
    }

    private Vertex newVertex(int i) {
        String text = "uniq" + i + " grp" + (i % GROUP_NUM) + " topic" + (i % 17)
            + " some filler content for verbalization cost";
        return new Vertex(LABEL, "id" + i, Collections.singletonList(text));
    }

    private EntityAttributeIndexStore newIndexStore(LocalMemoryGraphAccessor accessor) {
        EntityAttributeIndexStore store = new EntityAttributeIndexStore();
        store.initStore(new SubgraphSemanticPromptFunction(accessor));
        return store;
    }

    private List<String> buildQueries() {
        List<String> queries = new ArrayList<>(QUERY_NUM);
        for (int i = 0; i < QUERY_NUM; i++) {
            queries.add("grp" + (i * 37 % GROUP_NUM));
        }
        return queries;
    }

    private static String ms(long nanos) {
        return String.format("%.2f", nanos / 1_000_000.0);
    }

    private static Set<String> idsOf(List<GraphEntity> entities) {
        Set<String> ids = new HashSet<>();
        for (GraphEntity entity : entities) {
            Assertions.assertTrue(entity instanceof GraphVertex);
            ids.add(((GraphVertex) entity).getVertex().getId());
        }
        return ids;
    }

    @Test
    public void testResidentIndexIsEquivalentAndFaster() {
        LocalMemoryGraphAccessor accessor = buildGraph(VERTEX_NUM);
        List<String> queries = buildQueries();

        // Warm up the JIT and the Lucene classes, otherwise the first measured configuration pays
        // for class loading and interpretation and the comparison is meaningless.
        EntityAttributeIndexStore warmupStore = newIndexStore(accessor);
        SessionOperator warmupOperator = new SessionOperator(accessor, warmupStore);
        for (int i = 0; i < 3; i++) {
            warmupStore.invalidateCache();
            warmupOperator.searchWithGlobalGraphByRebuild(queries.get(0));
        }

        // Baseline: rebuild the index per query, and drop the verbalization cache each time so the
        // measurement reflects the original behaviour.
        EntityAttributeIndexStore coldStore = newIndexStore(accessor);
        SessionOperator coldOperator = new SessionOperator(accessor, coldStore);
        Map<String, Set<String>> baselineResults = new HashMap<>();
        long baselineCost = 0L;
        for (String query : queries) {
            coldStore.invalidateCache();
            long start = System.nanoTime();
            List<GraphEntity> result = coldOperator.searchWithGlobalGraphByRebuild(query);
            baselineCost += System.nanoTime() - start;
            baselineResults.put(query, idsOf(result));
        }

        // Rebuild per query, but keep the verbalization cache.
        EntityAttributeIndexStore cachedStore = newIndexStore(accessor);
        SessionOperator cachedOperator = new SessionOperator(accessor, cachedStore);
        long cachedCost = 0L;
        for (String query : queries) {
            long start = System.nanoTime();
            List<GraphEntity> result = cachedOperator.searchWithGlobalGraphByRebuild(query);
            cachedCost += System.nanoTime() - start;
            Assertions.assertEquals(baselineResults.get(query), idsOf(result),
                "verbalization cache must not change recall for query " + query);
        }

        // Resident index reused across queries.
        EntityAttributeIndexStore residentStore = newIndexStore(accessor);
        ResidentSearchIndex residentIndex = new ResidentSearchIndex();
        long residentCost = 0L;
        long residentFirstCost = 0L;
        long residentSteadyCost = 0L;
        for (int i = 0; i < queries.size(); i++) {
            String query = queries.get(i);
            long start = System.nanoTime();
            residentIndex.ensureGlobalIndex(accessor, residentStore);
            List<GraphEntity> result = residentIndex.search(query, accessor);
            long cost = System.nanoTime() - start;
            residentCost += cost;
            if (i == 0) {
                residentFirstCost = cost;
            } else {
                residentSteadyCost += cost;
            }
            Assertions.assertEquals(baselineResults.get(query), idsOf(result),
                "resident index must not change recall for query " + query);
        }

        // The whole point: the full graph index is built once, not once per query.
        Assertions.assertEquals(1L, residentIndex.getBuildCount());
        Assertions.assertEquals(VERTEX_NUM, residentIndex.getIndexedEntityNum());

        // Every query must have actually matched something, otherwise the comparison is vacuous.
        for (String query : queries) {
            Assertions.assertFalse(baselineResults.get(query).isEmpty(),
                "query matched nothing: " + query);
        }

        LOGGER.info("=== retrieval cost, vertices={}, queries={} ===", VERTEX_NUM, queries.size());
        LOGGER.info("[A] rebuild per query, no verbalization cache : total {} ms, avg {} ms/query",
            ms(baselineCost), ms(baselineCost / queries.size()));
        LOGGER.info("[B] rebuild per query, verbalization cached   : total {} ms, avg {} ms/query",
            ms(cachedCost), ms(cachedCost / queries.size()));
        LOGGER.info("[C] resident index                            : total {} ms, "
                + "first query (includes one time build) {} ms, steady state avg {} ms/query",
            ms(residentCost), ms(residentFirstCost), ms(residentSteadyCost / (queries.size() - 1)));
        LOGGER.info("verbalization cache in [B]: hit={}, miss={}, size={}",
            cachedStore.getCacheHit(), cachedStore.getCacheMiss(), cachedStore.getCacheSize());

        Assertions.assertTrue(residentCost < baselineCost,
            "resident index should be cheaper than rebuilding per query, baseline=" + baselineCost
                + "ns resident=" + residentCost + "ns");
    }

    /**
     * Shows the shape of the problem: the rebuild path grows with the graph, the resident path does
     * not. Only logged, not asserted, so the test stays stable on shared CI machines.
     */
    @Test
    public void testSteadyStateCostDoesNotGrowWithGraphSize() {
        for (int vertexNum : new int[] {5000, 20000}) {
            LocalMemoryGraphAccessor accessor = buildGraph(vertexNum);
            List<String> queries = buildQueries();

            EntityAttributeIndexStore coldStore = newIndexStore(accessor);
            SessionOperator coldOperator = new SessionOperator(accessor, coldStore);
            long rebuildCost = 0L;
            for (String query : queries) {
                coldStore.invalidateCache();
                long start = System.nanoTime();
                coldOperator.searchWithGlobalGraphByRebuild(query);
                rebuildCost += System.nanoTime() - start;
            }

            EntityAttributeIndexStore residentStore = newIndexStore(accessor);
            ResidentSearchIndex residentIndex = new ResidentSearchIndex();
            residentIndex.ensureGlobalIndex(accessor, residentStore);
            long steadyCost = 0L;
            for (String query : queries) {
                long start = System.nanoTime();
                residentIndex.search(query, accessor);
                steadyCost += System.nanoTime() - start;
            }
            Assertions.assertEquals(1L, residentIndex.getBuildCount());

            LOGGER.info("vertices={} : rebuild avg {} ms/query, resident steady avg {} ms/query",
                vertexNum, ms(rebuildCost / queries.size()), ms(steadyCost / queries.size()));
        }
    }

    @Test
    public void testVerbalizationCacheIsUsed() {
        LocalMemoryGraphAccessor accessor = buildGraph(100);
        EntityAttributeIndexStore store = newIndexStore(accessor);
        GraphVertex vertex = accessor.getVertex(LABEL, "id7");
        Assertions.assertNotNull(vertex);

        Assertions.assertEquals(store.getEntityIndex(vertex).toString(),
            store.getEntityIndex(vertex).toString());
        Assertions.assertEquals(1L, store.getCacheMiss());
        Assertions.assertEquals(1L, store.getCacheHit());

        store.invalidateCache(vertex);
        store.getEntityIndex(vertex);
        Assertions.assertEquals(2L, store.getCacheMiss());
    }

    @Test
    public void testInsertIsSearchableWithoutRebuild() {
        LocalMemoryGraphAccessor accessor = buildGraph(200);
        EntityAttributeIndexStore store = newIndexStore(accessor);
        ResidentSearchIndex residentIndex = new ResidentSearchIndex();
        residentIndex.ensureGlobalIndex(accessor, store);
        Assertions.assertEquals(1L, residentIndex.getBuildCount());
        Assertions.assertTrue(residentIndex.search("zebrafish", accessor).isEmpty());

        Vertex fresh = new Vertex(LABEL, "id-fresh",
            Collections.singletonList("zebrafish appears only here"));
        accessor.getMutableGraph().addVertex(fresh);
        residentIndex.onEntitiesUpserted(accessor, entities(fresh), store);

        Assertions.assertEquals(Collections.singleton("id-fresh"),
            idsOf(residentIndex.search("zebrafish", accessor)));
        Assertions.assertEquals(1L, residentIndex.getUpsertCount());
        Assertions.assertEquals(201, residentIndex.getIndexedEntityNum());
        assertNoRebuild(residentIndex, accessor, store);
    }

    @Test
    public void testUpdateIsAppliedInPlaceWithoutRebuild() {
        LocalMemoryGraphAccessor accessor = buildGraph(50);
        EntityAttributeIndexStore store = newIndexStore(accessor);
        ResidentSearchIndex residentIndex = new ResidentSearchIndex();
        residentIndex.ensureGlobalIndex(accessor, store);
        Assertions.assertEquals(Collections.singleton("id7"),
            idsOf(residentIndex.search("uniq7", accessor)));

        Vertex updated = new Vertex(LABEL, "id7", Collections.singletonList("narwhal now"));
        accessor.getMutableGraph().updateVertex(updated);
        store.invalidateCache(new GraphVertex(updated));
        residentIndex.onEntitiesUpserted(accessor, entities(updated), store);

        // New content is visible, the superseded document is gone, and the doc count is unchanged.
        Assertions.assertEquals(Collections.singleton("id7"),
            idsOf(residentIndex.search("narwhal", accessor)));
        Assertions.assertTrue(residentIndex.search("uniq7", accessor).isEmpty(),
            "the replaced document must no longer be searchable");
        Assertions.assertEquals(50, residentIndex.getIndexedEntityNum());
        assertNoRebuild(residentIndex, accessor, store);
    }

    @Test
    public void testDeleteIsAppliedInPlaceWithoutRebuild() {
        LocalMemoryGraphAccessor accessor = buildGraph(50);
        EntityAttributeIndexStore store = newIndexStore(accessor);
        ResidentSearchIndex residentIndex = new ResidentSearchIndex();
        residentIndex.ensureGlobalIndex(accessor, store);
        Assertions.assertEquals(Collections.singleton("id9"),
            idsOf(residentIndex.search("uniq9", accessor)));

        Vertex removed = accessor.getVertex(LABEL, "id9").getVertex();
        accessor.getMutableGraph().removeVertex(LABEL, "id9");
        residentIndex.onEntitiesRemoved(accessor, entities(removed));

        Assertions.assertTrue(residentIndex.search("uniq9", accessor).isEmpty(),
            "the deleted document must no longer be searchable");
        Assertions.assertEquals(1L, residentIndex.getRemoveCount());
        Assertions.assertEquals(49, residentIndex.getIndexedEntityNum());
        assertNoRebuild(residentIndex, accessor, store);
    }

    @Test
    public void testUpsertIsIdempotent() {
        LocalMemoryGraphAccessor accessor = buildGraph(20);
        EntityAttributeIndexStore store = newIndexStore(accessor);
        ResidentSearchIndex residentIndex = new ResidentSearchIndex();
        residentIndex.ensureGlobalIndex(accessor, store);

        Vertex existing = accessor.getVertex(LABEL, "id3").getVertex();
        for (int i = 0; i < 3; i++) {
            residentIndex.onEntitiesUpserted(accessor, entities(existing), store);
        }

        // Replaying the same write must not duplicate the document nor trigger a rebuild.
        Assertions.assertEquals(Collections.singleton("id3"),
            idsOf(residentIndex.search("uniq3", accessor)));
        Assertions.assertEquals(20, residentIndex.getIndexedEntityNum());
        assertNoRebuild(residentIndex, accessor, store);
    }

    @Test
    public void testMutationOutsideTheIndexForcesRebuild() {
        LocalMemoryGraphAccessor accessor = buildGraph(20);
        EntityAttributeIndexStore store = newIndexStore(accessor);
        ResidentSearchIndex residentIndex = new ResidentSearchIndex();
        residentIndex.ensureGlobalIndex(accessor, store);
        Assertions.assertEquals(1L, residentIndex.getBuildCount());

        // Graph changed without notifying the index; only the version guard can catch this.
        accessor.getMutableGraph().addVertex(new Vertex(LABEL, "id-hidden",
            Collections.singletonList("okapi appears only here")));

        Assertions.assertEquals(Collections.singleton("id-hidden"),
            idsOf(residentIndex.searchWithIndex(accessor, store, "okapi")));
        Assertions.assertEquals(2L, residentIndex.getBuildCount(),
            "the version guard must force a rebuild for unnotified mutations");
    }

    @Test
    public void testEdgeWriteDoesNotInvalidateVertexIndex() {
        LocalMemoryGraphAccessor accessor = buildGraph(20);
        EntityAttributeIndexStore store = newIndexStore(accessor);
        ResidentSearchIndex residentIndex = new ResidentSearchIndex();
        residentIndex.ensureGlobalIndex(accessor, store);

        accessor.getMutableGraph().addEdgeSchema(
            new EdgeSchema(EDGE_LABEL, "srcId", "dstId", Collections.singletonList("rel")));
        residentIndex.ensureGlobalIndex(accessor, store);
        // The schema change is a vertex level change, so a rebuild is expected here.
        long afterSchema = residentIndex.getBuildCount();

        accessor.getMutableGraph().addEdge(
            new Edge(EDGE_LABEL, "id1", "id2", Collections.singletonList("linked")));
        residentIndex.ensureGlobalIndex(accessor, store);
        Assertions.assertEquals(afterSchema, residentIndex.getBuildCount(),
            "an edge write must not invalidate a vertex only index");
    }

    /**
     * The write heavy scenario in place maintenance exists for: writes interleaved with queries.
     * With invalidate-on-write every query after a write pays a full rebuild; with in place
     * maintenance the index is built once and never again.
     */
    @Test
    public void testInterleavedWritesAndQueriesNeverRebuild() {
        // Kept smaller than the read benchmark: the reference configuration rebuilds the whole
        // index on every round, so its cost is rounds x O(V).
        int vertexNum = 5000;
        int rounds = 40;

        LocalMemoryGraphAccessor inPlaceAccessor = buildGraph(vertexNum);
        EntityAttributeIndexStore inPlaceStore = newIndexStore(inPlaceAccessor);
        ResidentSearchIndex inPlaceIndex = new ResidentSearchIndex();
        inPlaceIndex.ensureGlobalIndex(inPlaceAccessor, inPlaceStore);

        LocalMemoryGraphAccessor invalidateAccessor = buildGraph(vertexNum);
        EntityAttributeIndexStore invalidateStore = newIndexStore(invalidateAccessor);
        ResidentSearchIndex invalidateIndex = new ResidentSearchIndex();
        invalidateIndex.ensureGlobalIndex(invalidateAccessor, invalidateStore);

        long inPlaceCost = 0L;
        long invalidateCost = 0L;
        for (int i = 0; i < rounds; i++) {
            Vertex fresh = new Vertex(LABEL, "id-new" + i,
                Collections.singletonList("grp" + (i % GROUP_NUM) + " freshly written " + i));
            String query = "grp" + (i % GROUP_NUM);

            inPlaceAccessor.getMutableGraph().addVertex(fresh);
            long start = System.nanoTime();
            inPlaceIndex.onEntitiesUpserted(inPlaceAccessor, entities(fresh), inPlaceStore);
            List<GraphEntity> inPlaceHit = inPlaceIndex.searchWithIndex(inPlaceAccessor,
                inPlaceStore, query);
            inPlaceCost += System.nanoTime() - start;

            // Reference behaviour: drop the index on every write and rebuild lazily.
            invalidateAccessor.getMutableGraph().addVertex(fresh);
            start = System.nanoTime();
            invalidateIndex.invalidate();
            List<GraphEntity> invalidateHit = invalidateIndex.searchWithIndex(invalidateAccessor,
                invalidateStore, query);
            invalidateCost += System.nanoTime() - start;

            Assertions.assertEquals(idsOf(invalidateHit), idsOf(inPlaceHit),
                "in place maintenance must recall the same entities as rebuilding, round " + i);
            Assertions.assertTrue(idsOf(inPlaceHit).contains("id-new" + i),
                "the just written vertex must be visible, round " + i);
        }

        Assertions.assertEquals(1L, inPlaceIndex.getBuildCount(),
            "in place maintenance must never rebuild");
        Assertions.assertEquals(1L + rounds, invalidateIndex.getBuildCount());
        Assertions.assertEquals(rounds, inPlaceIndex.getUpsertCount());
        Assertions.assertEquals(vertexNum + rounds, inPlaceIndex.getIndexedEntityNum());

        LOGGER.info("=== interleaved write + query, vertices={}, rounds={} ===", vertexNum, rounds);
        LOGGER.info("[D] invalidate on write : total {} ms, avg {} ms/round, builds {}",
            ms(invalidateCost), ms(invalidateCost / rounds), invalidateIndex.getBuildCount());
        LOGGER.info("[E] in place maintenance: total {} ms, avg {} ms/round, builds {}",
            ms(inPlaceCost), ms(inPlaceCost / rounds), inPlaceIndex.getBuildCount());
    }

    private static List<GraphEntity> entities(Vertex... vertices) {
        List<GraphEntity> list = new ArrayList<>(vertices.length);
        for (Vertex vertex : vertices) {
            list.add(new GraphVertex(vertex));
        }
        return list;
    }

    private static void assertNoRebuild(ResidentSearchIndex residentIndex,
                                        LocalMemoryGraphAccessor accessor,
                                        EntityAttributeIndexStore store) {
        residentIndex.ensureGlobalIndex(accessor, store);
        Assertions.assertEquals(1L, residentIndex.getBuildCount(),
            "in place maintenance must keep the accepted version in step, no rebuild expected");
    }
}
