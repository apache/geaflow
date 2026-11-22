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

package org.apache.geaflow.ai.retrieval.traversal;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Random;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.geaflow.ai.graph.*;
import org.apache.geaflow.ai.graph.io.*;
import org.apache.geaflow.ai.retrieval.traversal.GraphTraversalRecall;
import org.apache.geaflow.ai.retrieval.traversal.GraphTraversalRecall.*;

/** Traversal contracts backed by the real local graph accessor. */
public class GraphTraversalRecallTest {
    private final List<DynamicTest> cases = new ArrayList<>();
    private static final GraphVertex A = vertex("a");
    private static final GraphVertex B = vertex("b");
    private static final GraphVertex C = vertex("c");
    private static final GraphVertex D = vertex("d");
    private static final Limits LIMITS = new Limits(3, 100, 100, 100, 1000);

    @TestFactory
    public Collection<DynamicTest> graphContracts() {
        Fixture chain = new Fixture(edge(A, B), edge(B, C), edge(C, D));
        Result r = run(chain, Collections.singletonList(A), LIMITS);
        check("three-hop discovery and provenance", r.hits.size() == 3
            && hit(r, D).path.size() == 3 && hit(r, D).path.get(0).getEdge().getSrcId().equals("a"));
        Result repeat = run(chain, Collections.singletonList(A), LIMITS);
        check("fixed-seed exact repeatability", signature(r).equals(signature(repeat)));
        List<GraphEdge> reversed = new ArrayList<>(chain.edges);
        Collections.reverse(reversed);
        check("adjacency-order independence", signature(r).equals(signature(
            run(new Fixture(reversed.toArray(new GraphEdge[0])), Collections.singletonList(A), LIMITS))));
        Result shallow = run(chain, Collections.singletonList(A), new Limits(1, 100, 100, 100, 100));
        check("hop bound", shallow.hits.size() == 1 && hit(shallow, B).path.size() == 1
            && shallow.hopLimitReached);
        Result empty = run(new Fixture(), Collections.singletonList(A), LIMITS);
        check("empty graph", empty.hits.isEmpty() && empty.scannedEdges == 0);
        Result self = run(new Fixture(edge(A, A), edge(A, B)), Collections.singletonList(A), LIMITS);
        check("anchor exclusion", self.hits.size() == 1 && self.hits.get(0).candidate.equals(B));
        Result vertices = run(chain, Collections.singletonList(A), new Limits(3, 1, 100, 100, 100));
        check("vertex budget", vertices.stopReason == StopReason.VERTEX_LIMIT
            && vertices.visitedVertices == 1 && vertices.traversedEdges == 0);
        Result edges = run(new Fixture(edge(A, B), edge(A, C)), Collections.singletonList(A),
            new Limits(3, 100, 1, 100, 100));
        check("edge-scan budget", edges.stopReason == StopReason.EDGE_SCAN_LIMIT && edges.scannedEdges == 1);
        Result top = run(chain, Collections.singletonList(A), new Limits(3, 100, 100, 1, 1000));
        check("result truncation after ranking", top.hits.size() == 1
            && top.hits.get(0).candidate.equals(r.hits.get(0).candidate) && top.resultsTruncated);
        Result multi = run(new Fixture(edge(A, B), edge(C, B)), Arrays.asList(A, C), LIMITS);
        check("multiple anchors deduplicate candidates", multi.hits.size() == 1
            && multi.hits.get(0).candidate.equals(B));
        Result visits = new GraphTraversalRecall().recall(new Fixture(edge(A, B)),
            Collections.singletonList(A), new Limits(1, 10, 10, 10, 10), 7, 0,
            e -> 1, 0, () -> 0L, () -> false);
        check("exact visit-frequency score", visits.hits.get(0).visits == 5
            && visits.hits.get(0).score == 0.5 && visits.traversedEdges == 5);
        Result weighted = new GraphTraversalRecall().recall(new Fixture(edge(A, B), edge(A, C)),
            Collections.singletonList(A), LIMITS, 7, 0.2,
            e -> e.getEdge().getDstId().equals("b") ? Double.MAX_VALUE : 0,
            0, () -> 0L, () -> false);
        check("zero weight and overflow-safe weighting", weighted.hits.size() == 1
            && weighted.hits.get(0).candidate.equals(B));
        Result cancelled = new GraphTraversalRecall().recall(chain, Collections.singletonList(A),
            LIMITS, 7, 0.2, e -> 1, 0, () -> 0L, () -> true);
        check("pre-cancelled request", cancelled.stopReason == StopReason.CANCELLED
            && cancelled.scannedEdges == 0);
        AtomicInteger ticks = new AtomicInteger();
        Result timeout = new GraphTraversalRecall().recall(chain, Collections.singletonList(A),
            LIMITS, 7, 0.2, e -> 1, 5, () -> ticks.incrementAndGet(), () -> false);
        check("injected deadline", timeout.stopReason == StopReason.DEADLINE);
        Result cycles = run(new Fixture(edge(A, B), edge(B, C), edge(C, A)),
            Collections.singletonList(A), LIMITS);
        check("cycles terminate and paths stay bounded", cycles.hits.size() == 2
            && cycles.traversedEdges <= LIMITS.stepsPerAnchor
            && cycles.hits.stream().allMatch(h -> h.path.size() <= LIMITS.maxHops));
        for (Strategy strategy : Arrays.asList(Strategy.BREADTH_FIRST, Strategy.DEPTH_FIRST)) {
            Result search = new GraphTraversalRecall().recall(chain, Collections.singletonList(A),
                LIMITS, strategy, 7, 0.2, e -> 1, 0, () -> 0L, () -> false);
            check(strategy + " three-hop path", search.hits.size() == 3 && hit(search, D).path.size() == 3);
            Result bounded = new GraphTraversalRecall().recall(chain, Collections.singletonList(A),
                new Limits(1, 100, 100, 100, 100), strategy, 7, 0.2, e -> 1,
                0, () -> 0L, () -> false);
            check(strategy + " depth cap", bounded.hits.size() == 1 && hit(bounded, B).path.size() == 1);
        }
        GraphVertex x = vertex("x");
        GraphVertex z = vertex("z");
        Fixture revisit = new Fixture(edge(A, z), edge(z, x), edge(x, C), edge(A, B), edge(B, C), edge(C, D));
        Result dfs = new GraphTraversalRecall().recall(revisit, Collections.singletonList(A), LIMITS,
            Strategy.DEPTH_FIRST, 7, 0.2, e -> 1, 0, () -> 0L, () -> false);
        check("depth-limited DFS re-expands shallower arrival", hit(dfs, D).path.size() == 3);
        Result traversals = run(chain, Collections.singletonList(A), new Limits(3, 100, 100, 2, 100, 100));
        check("independent traversal budget", traversals.stopReason == StopReason.EDGE_TRAVERSAL_LIMIT
            && traversals.traversedEdges == 2);
        check("duplicate anchors do not inflate score", signature(r).equals(signature(
            run(chain, Arrays.asList(A, A), LIMITS))));
        Result twoSeeds = new GraphTraversalRecall().recall(new Fixture(edge(A, B), edge(C, B)),
            Arrays.asList(A, C), new Limits(1, 100, 100, 100, 10), 7, 0, e -> 1,
            0, () -> 0L, () -> false);
        check("multi-anchor score accumulates exact frequencies", twoSeeds.hits.size() == 1
            && hit(twoSeeds, B).visits == 10 && hit(twoSeeds, B).score == 1.0);
        check("no anchors", run(chain, Collections.emptyList(), LIMITS).hits.isEmpty());
        boolean invalidWeight = false;
        try {
            new GraphTraversalRecall().recall(chain, Collections.singletonList(A), LIMITS,
                7, 0.2, e -> -1, 0, () -> 0L, () -> false);
        } catch (IllegalArgumentException expected) {
            invalidWeight = true;
        }
        check("negative weights rejected", invalidWeight);
        Result distribution = new GraphTraversalRecall().recall(new Fixture(edge(A, B), edge(A, C)),
            Collections.singletonList(A), new Limits(1, 100, 100, 100, 20000), 7, 0,
            e -> e.getEdge().getDstId().equals("b") ? 1 : 3, 0, () -> 0L, () -> false);
        double share = (double) hit(distribution, B).visits / distribution.traversedEdges;
        check("weighted CDF matches synthetic 1:3 distribution", Math.abs(share - 0.25) < 0.02);
        Result hugeWeights = new GraphTraversalRecall().recall(new Fixture(edge(A, B), edge(A, C)),
            Collections.singletonList(A), new Limits(1, 100, 100, 100, 10000), 7, 0,
            e -> Double.MAX_VALUE, 0, () -> 0L, () -> false);
        double hugeShare = (double) hit(hugeWeights, B).visits / hugeWeights.traversedEdges;
        check("two maximum finite weights retain equal probabilities", Math.abs(hugeShare - 0.5) < 0.03);
        Result onlyLoop = run(new Fixture(edge(A, A)), Collections.singletonList(A), LIMITS);
        check("self-loop consumes walks without emitting anchor", onlyLoop.hits.isEmpty()
            && onlyLoop.traversedEdges > 0);
        return cases;
    }

    private static Result run(Fixture graph, List<GraphVertex> anchors, Limits limits) {
        return new GraphTraversalRecall().recall(graph, anchors, limits, 7, 0.2,
            e -> 1, 0, () -> 0L, () -> false);
    }

    private static Hit hit(Result r, GraphVertex v) {
        return r.hits.stream().filter(h -> h.candidate.equals(v)).findFirst().orElseThrow(AssertionError::new);
    }

    private static String signature(Result r) {
        StringBuilder text = new StringBuilder();
        for (Hit h : r.hits) {
            text.append(h.candidate).append(h.score).append(h.visits).append(h.path);
        }
        return text.toString();
    }

    private void check(String name, boolean condition) {
        cases.add(DynamicTest.dynamicTest(name, () -> assertTrue(condition, name)));
    }

    @Test
    public void selfLoopIsOneWeightedChoiceDespiteRepeatedAdjacencyRecords() {
        Fixture graph = new Fixture(edge(A, A), edge(A, B));
        int records = 0;
        Iterator<GraphEdge> iterator = graph.scanEdge(A);
        while (iterator.hasNext()) {
            iterator.next();
            records++;
        }
        // LocalMemoryGraphAccessor emits a self-loop four times (two in, two out).
        assertEquals(5, records);
        Result result = new GraphTraversalRecall().recall(graph, Collections.singletonList(A),
            new Limits(1, 100, 100, 100, 20000), 7, 0, e -> 1,
            0, () -> 0L, () -> false);
        double share = (double) hit(result, B).visits / result.traversedEdges;
        assertTrue(Math.abs(share - 0.5) < 0.02, "observed candidate share: " + share);
    }

    @Test
    public void preservesSeparateParallelEdgeWeights() {
        GraphEdge first = new GraphEdge(new Edge("link", "a", "b", Arrays.asList("1")));
        GraphEdge second = new GraphEdge(new Edge("link", "a", "b", Arrays.asList("2")));
        GraphEdge third = new GraphEdge(new Edge("link", "a", "c", Arrays.asList("3")));
        Fixture graph = new Fixture(first, second, third);
        Result result = new GraphTraversalRecall().recall(graph, Collections.singletonList(A),
            new Limits(1, 100, 100, 100, 20000), 7, 0,
            e -> Double.parseDouble(e.getEdge().getValues().get(0)),
            0, () -> 0L, () -> false);
        double share = (double) hit(result, B).visits / result.traversedEdges;
        assertTrue(Math.abs(share - 0.5) < 0.02, "observed parallel-edge share: " + share);
        Fixture reverse = new Fixture(third, second, first);
        Result reversed = new GraphTraversalRecall().recall(reverse, Collections.singletonList(A),
            new Limits(1, 100, 100, 100, 20000), 7, 0,
            e -> Double.parseDouble(e.getEdge().getValues().get(0)),
            0, () -> 0L, () -> false);
        assertEquals(signature(result), signature(reversed));
    }

    @Test
    public void distinctSelfLoopObjectsRetainIndependentWeights() {
        Fixture graph = new Fixture(edge(A, A), edge(A, A), edge(A, B));
        Result result = new GraphTraversalRecall().recall(graph, Collections.singletonList(A),
            new Limits(1, 100, 100, 100, 20000), 7, 0, e -> 1,
            0, () -> 0L, () -> false);
        double share = (double) hit(result, B).visits / result.traversedEdges;
        assertTrue(Math.abs(share - 1.0 / 3) < 0.02, "observed distinct-loop share: " + share);
    }

    @Test
    public void searchesMatchBoundedReachabilityOnSeededGraphs() {
        Random random = new Random(861);
        for (int example = 0; example < 100; example++) {
            int size = 7;
            int hops = 1 + example % 4;
            List<GraphEdge> edges = new ArrayList<>();
            for (int source = 0; source < size; source++) {
                for (int target = source; target < size; target++) {
                    if (random.nextDouble() < 0.3) {
                        edges.add(edge(vertex(Integer.toString(source)), vertex(Integer.toString(target))));
                    }
                }
            }
            // Independent bounded relaxation oracle, using the fixture's undirected adjacency.
            int[] distance = new int[size];
            Arrays.fill(distance, 1000);
            distance[0] = 0;
            for (int depth = 0; depth < hops; depth++) {
                int[] next = distance.clone();
                for (GraphEdge edge : edges) {
                    int source = Integer.parseInt(edge.getEdge().getSrcId());
                    int target = Integer.parseInt(edge.getEdge().getDstId());
                    next[target] = Math.min(next[target], distance[source] + 1);
                    next[source] = Math.min(next[source], distance[target] + 1);
                }
                distance = next;
            }
            for (Strategy strategy : Arrays.asList(Strategy.BREADTH_FIRST, Strategy.DEPTH_FIRST)) {
                Result result = new GraphTraversalRecall().recall(
                    new Fixture(edges.toArray(new GraphEdge[0])), Collections.singletonList(vertex("0")),
                    new Limits(hops, 100, 10000, 10000, 100, 100), strategy, 7, 0.2,
                    e -> 1, 0, () -> 0L, () -> false);
                Map<String, Hit> actual = new LinkedHashMap<>();
                for (Hit hit : result.hits) {
                    actual.put(hit.candidate.getVertex().getId(), hit);
                }
                int expectedSize = 0;
                for (int id = 1; id < size; id++) {
                    if (distance[id] <= hops) {
                        expectedSize++;
                        Hit hit = actual.get(Integer.toString(id));
                        assertTrue(hit != null, strategy + " missed node in example " + example);
                        assertEquals(distance[id], hit.path.size(), strategy + " path depth");
                        String current = "0";
                        for (GraphEdge edge : hit.path) {
                            String source = edge.getEdge().getSrcId();
                            String target = edge.getEdge().getDstId();
                            assertTrue(current.equals(source) || current.equals(target));
                            current = current.equals(source) ? target : source;
                        }
                        assertEquals(Integer.toString(id), current);
                    }
                }
                assertEquals(expectedSize, actual.size(), strategy + " candidate set");
            }
        }
    }

    private static GraphVertex vertex(String id) {
        return new GraphVertex(new Vertex("node", id, Collections.emptyList()));
    }

    private static GraphEdge edge(GraphVertex source, GraphVertex target) {
        return new GraphEdge(new Edge("link", source.getVertex().getId(),
            target.getVertex().getId(), Collections.emptyList()));
    }

    private static final class Fixture implements GraphAccessor {
        private final List<GraphEdge> edges;
        private final LocalMemoryGraphAccessor delegate;

        private Fixture(GraphEdge... values) {
            edges = Arrays.asList(values);
            Map<String, Vertex> vertices = new LinkedHashMap<>();
            vertices.put("a", A.getVertex());
            List<Edge> innerEdges = new ArrayList<>();
            for (GraphEdge edge : edges) {
                innerEdges.add(edge.getEdge());
                vertices.put(edge.getEdge().getSrcId(), vertex(edge.getEdge().getSrcId()).getVertex());
                vertices.put(edge.getEdge().getDstId(), vertex(edge.getEdge().getDstId()).getVertex());
            }
            VertexSchema vertexSchema = new VertexSchema("node", "id", Collections.emptyList());
            EdgeSchema edgeSchema = new EdgeSchema("link", "src", "dst", Collections.emptyList());
            GraphSchema schema = new GraphSchema(Collections.singletonList(vertexSchema),
                Collections.singletonList(edgeSchema));
            Map<String, EntityGroup> groups = new LinkedHashMap<>();
            groups.put("node", new VertexGroup(vertexSchema, new ArrayList<>(vertices.values())));
            groups.put("link", new EdgeGroup(edgeSchema, innerEdges));
            delegate = new LocalMemoryGraphAccessor(new MemoryGraph(schema, groups));
        }

        public GraphSchema getGraphSchema() { return delegate.getGraphSchema(); }
        public GraphVertex getVertex(String label, String id) { return delegate.getVertex(label, id); }
        public List<GraphEdge> getEdge(String label, String src, String dst) {
            return delegate.getEdge(label, src, dst);
        }
        public Iterator<GraphVertex> scanVertex() { return delegate.scanVertex(); }
        public Iterator<GraphEdge> scanEdge(GraphVertex v) { return delegate.scanEdge(v); }
        public List<GraphEntity> expand(GraphEntity entity) { return delegate.expand(entity); }
        public GraphAccessor copy() { return delegate.copy(); }
        public String getType() { return delegate.getType(); }
    }
}
