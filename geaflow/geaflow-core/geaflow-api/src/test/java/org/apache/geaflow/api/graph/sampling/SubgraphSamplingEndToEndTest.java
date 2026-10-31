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

package org.apache.geaflow.api.graph.sampling;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.geaflow.model.graph.edge.EdgeDirection;
import org.apache.geaflow.model.graph.edge.IEdge;
import org.apache.geaflow.model.graph.edge.impl.ValueEdge;
import org.apache.geaflow.model.graph.vertex.impl.ValueVertex;
import org.apache.geaflow.state.sampling.DeterministicNeighborSampler;
import org.apache.geaflow.state.sampling.LocalNeighborhood;
import org.testng.Assert;
import org.testng.annotations.Test;

public class SubgraphSamplingEndToEndTest {

    private static final long SNAPSHOT_VERSION = 7L;
    private static final long SAMPLING_VERSION = 42L;
    private static final long SEED = 17L;

    @Test
    public void testRunsThreeHopSamplingWithOutOfOrderResponses() {
        SubgraphSamplingSpec spec = new SubgraphSamplingSpec(
            3, -1, EdgeDirection.OUT, 100L, SEED);
        RecordingGraph graph = diamondAndCycleGraph();
        SamplingDriver driver = new SamplingDriver(graph, spec);

        SampledSubgraph<Long, Integer, Integer> result = driver.run(1L);

        Assert.assertEquals(new LinkedHashSet<>(driver.getRequestTrace().subList(0, 2)),
            new LinkedHashSet<>(Arrays.asList("1->2@1", "1->3@1")));
        Assert.assertEquals(new LinkedHashSet<>(driver.getRequestTrace().subList(2, 4)),
            new LinkedHashSet<>(Arrays.asList("1->4@2", "1->5@2")));
        Assert.assertEquals(driver.getRequestTrace().get(4), "1->6@3");
        Assert.assertNotEquals(driver.getRequestTrace().subList(0, 2),
            driver.getEnqueuedRequestTrace().subList(0, 2));
        Assert.assertEquals(new LinkedHashSet<>(driver.getResponseTrace().subList(0, 2)),
            new LinkedHashSet<>(Arrays.asList("2->1@1", "3->1@1")));
        Assert.assertEquals(new LinkedHashSet<>(driver.getResponseTrace().subList(2, 4)),
            new LinkedHashSet<>(Arrays.asList("4->1@2", "5->1@2")));
        Assert.assertEquals(driver.getResponseTrace().get(4), "6->1@3");
        Assert.assertEquals(driver.getTerminalResponseEdgeCounts(), Collections.singletonList(0));
        Assert.assertEquals(driver.getRequestDepths(), Arrays.asList(1, 1, 2, 2, 3));
        Assert.assertEquals(driver.getResponseCount(), driver.getRequestCount());
        Assert.assertEquals(driver.getRequestCount(), 5);
        Assert.assertEquals(result.getVertices().keySet(), new LinkedHashSet<>(Arrays.asList(
            1L, 2L, 3L, 4L, 5L, 6L)));
        Assert.assertEquals(result.getEdgeLayers().size(), 3);
        Assert.assertEquals(result.getEdgeLayers().get(0).size(), 2);
        Assert.assertEquals(result.getEdgeLayers().get(1).size(), 4);
        Assert.assertEquals(result.getEdgeLayers().get(2).size(), 3);
        Assert.assertEquals(graph.getSampleReads().size(), 5);
        Assert.assertEquals(graph.getVertexOnlyReads(), Collections.singletonList("6@3"));
    }

    @Test
    public void testDoesNotReadEdgesForTerminalDepth() {
        SubgraphSamplingSpec spec = new SubgraphSamplingSpec(
            2, -1, EdgeDirection.OUT, 100L, SEED);
        RecordingGraph graph = lineGraph();
        SamplingDriver driver = new SamplingDriver(graph, spec);

        SampledSubgraph<Long, Integer, Integer> result = driver.run(1L);

        Assert.assertEquals(graph.getSampleReads(), Arrays.asList("1@0", "2@1"));
        Assert.assertEquals(graph.getVertexOnlyReads(), Collections.singletonList("3@2"));
        Assert.assertEquals(driver.getTerminalResponseEdgeCounts(), Collections.singletonList(0));
        Assert.assertEquals(result.getVertices().keySet(), new LinkedHashSet<>(Arrays.asList(
            1L, 2L, 3L)));
        Assert.assertEquals(result.getEdgeLayers().size(), 2);
        Assert.assertEquals(result.getEdgeLayers().get(0).size(), 1);
        Assert.assertEquals(result.getEdgeLayers().get(1).size(), 1);
        Assert.assertEquals(driver.getRequestDepths(), Arrays.asList(1, 2));
    }

    @Test
    public void testEndToEndSamplingHonorsPositiveFanout() {
        SubgraphSamplingSpec spec = new SubgraphSamplingSpec(
            1, 1, EdgeDirection.OUT, 100L, SEED);
        RecordingGraph graph = fanoutGraph();
        SamplingDriver driver = new SamplingDriver(graph, spec);

        SampledSubgraph<Long, Integer, Integer> result = driver.run(1L);

        Assert.assertEquals(result.getVertices().size(), 2);
        Assert.assertEquals(result.getEdgeLayers().get(0).size(), 1);
        Assert.assertEquals(driver.getRequestCount(), 1);
        Assert.assertEquals(graph.getSampleReads(), Collections.singletonList("1@0"));
        Assert.assertEquals(graph.getVertexOnlyReads().size(), 1);
    }

    private static RecordingGraph diamondAndCycleGraph() {
        Map<Long, List<IEdge<Long, Integer>>> adjacency = new LinkedHashMap<>();
        adjacency.put(1L, Arrays.asList(edge(1L, 2L), edge(1L, 3L)));
        adjacency.put(2L, Arrays.asList(edge(2L, 4L), edge(2L, 5L)));
        adjacency.put(3L, Arrays.asList(edge(3L, 4L), edge(3L, 5L)));
        adjacency.put(4L, Arrays.asList(edge(4L, 1L), edge(4L, 6L)));
        adjacency.put(5L, Collections.singletonList(edge(5L, 6L)));
        adjacency.put(6L, Collections.emptyList());
        return new RecordingGraph(adjacency);
    }

    private static RecordingGraph lineGraph() {
        Map<Long, List<IEdge<Long, Integer>>> adjacency = new LinkedHashMap<>();
        adjacency.put(1L, Collections.singletonList(edge(1L, 2L)));
        adjacency.put(2L, Collections.singletonList(edge(2L, 3L)));
        adjacency.put(3L, Collections.emptyList());
        return new RecordingGraph(adjacency);
    }

    private static RecordingGraph fanoutGraph() {
        Map<Long, List<IEdge<Long, Integer>>> adjacency = new LinkedHashMap<>();
        adjacency.put(1L, Arrays.asList(edge(1L, 2L), edge(1L, 3L), edge(1L, 4L)));
        adjacency.put(2L, Collections.emptyList());
        adjacency.put(3L, Collections.emptyList());
        adjacency.put(4L, Collections.emptyList());
        return new RecordingGraph(adjacency);
    }

    private static IEdge<Long, Integer> edge(long source, long target) {
        return new ValueEdge<>(source, target, 1, EdgeDirection.OUT);
    }

    private static byte[] longBytes(long value) {
        return new byte[]{
            (byte) (value >>> 56), (byte) (value >>> 48), (byte) (value >>> 40),
            (byte) (value >>> 32), (byte) (value >>> 24), (byte) (value >>> 16),
            (byte) (value >>> 8), (byte) value};
    }

    private static final class SamplingDriver {

        private final RecordingGraph graph;
        private final SubgraphSamplingSpec spec;
        private final LayeredSubgraphAssembler<Long, Integer, Integer> assembler =
            new LayeredSubgraphAssembler<>();
        private final List<String> enqueuedRequestTrace = new ArrayList<>();
        private final List<String> requestTrace = new ArrayList<>();
        private final List<String> responseTrace = new ArrayList<>();
        private final List<Integer> requestDepths = new ArrayList<>();
        private final List<Integer> terminalResponseEdgeCounts = new ArrayList<>();

        private SamplingDriver(RecordingGraph graph, SubgraphSamplingSpec spec) {
            this.graph = graph;
            this.spec = spec;
        }

        private SampledSubgraph<Long, Integer, Integer> run(long rootId) {
            LocalNeighborhood<Long, Integer, Integer> rootNeighborhood =
                graph.sample(rootId, 0, spec);
            assembler.start(rootId, spec.getHops(), rootNeighborhood);
            Set<Long> frontier = neighbors(rootId, rootNeighborhood.getEdges());

            for (int depth = 1; depth <= spec.getHops(); depth++) {
                List<RoutedRequest> requests = new ArrayList<>();
                for (Long vertexId : frontier) {
                    if (assembler.registerRequest(rootId, vertexId, depth)) {
                        requests.add(new RoutedRequest(vertexId,
                            new SubgraphSamplingRequest<>(rootId, depth)));
                        enqueuedRequestTrace.add(rootId + "->" + vertexId + "@" + depth);
                    }
                }

                // Reverse delivery order to ensure assembly does not depend on transport order.
                Collections.reverse(requests);
                Set<Long> nextFrontier = new LinkedHashSet<>();
                for (RoutedRequest request : requests) {
                    Long requestRootId = request.request.getRootId();
                    int requestDepth = request.request.getDepth();
                    Assert.assertEquals(requestRootId, Long.valueOf(rootId));
                    Assert.assertEquals(requestDepth, depth);
                    requestTrace.add(requestRootId + "->" + request.vertexId + "@" + requestDepth);
                    requestDepths.add(requestDepth);
                    LocalNeighborhood<Long, Integer, Integer> neighborhood = requestDepth == spec.getHops()
                        ? graph.vertexOnly(request.vertexId, requestDepth)
                        : graph.sample(request.vertexId, requestDepth, spec);
                    responseTrace.add(request.vertexId + "->" + requestRootId + "@" + requestDepth);
                    if (requestDepth == spec.getHops()) {
                        terminalResponseEdgeCounts.add(neighborhood.getEdges().size());
                    }
                    SubgraphSamplingResponse<Long, Integer, Integer> response =
                        new SubgraphSamplingResponse<>(requestRootId, requestDepth, neighborhood);
                    Assert.assertTrue(assembler.add(response));
                    if (requestDepth < spec.getHops()) {
                        nextFrontier.addAll(neighbors(request.vertexId, neighborhood.getEdges()));
                    }
                }
                frontier = nextFrontier;
            }
            return assembler.take(rootId);
        }

        private Set<Long> neighbors(long vertexId, List<IEdge<Long, Integer>> edges) {
            Set<Long> neighbors = new LinkedHashSet<>();
            for (IEdge<Long, Integer> edge : edges) {
                if (Long.valueOf(vertexId).equals(edge.getSrcId())) {
                    neighbors.add(edge.getTargetId());
                } else {
                    neighbors.add(edge.getSrcId());
                }
            }
            return neighbors;
        }

        private List<String> getRequestTrace() {
            return requestTrace;
        }

        private List<String> getEnqueuedRequestTrace() {
            return enqueuedRequestTrace;
        }

        private List<String> getResponseTrace() {
            return responseTrace;
        }

        private List<Integer> getRequestDepths() {
            return requestDepths;
        }

        private int getRequestCount() {
            return requestTrace.size();
        }

        private int getResponseCount() {
            return responseTrace.size();
        }

        private List<Integer> getTerminalResponseEdgeCounts() {
            return terminalResponseEdgeCounts;
        }
    }

    private static final class RoutedRequest {

        private final Long vertexId;
        private final SubgraphSamplingRequest<Long> request;

        private RoutedRequest(Long vertexId, SubgraphSamplingRequest<Long> request) {
            this.vertexId = vertexId;
            this.request = request;
        }
    }

    private static final class RecordingGraph {

        private final Map<Long, List<IEdge<Long, Integer>>> adjacency;
        private final List<String> sampleReads = new ArrayList<>();
        private final List<String> vertexOnlyReads = new ArrayList<>();

        private RecordingGraph(Map<Long, List<IEdge<Long, Integer>>> adjacency) {
            this.adjacency = adjacency;
        }

        private LocalNeighborhood<Long, Integer, Integer> sample(
            long vertexId, int depth, SubgraphSamplingSpec spec) {
            if (depth >= spec.getHops()) {
                Assert.fail("terminal depth must not read or sample adjacent edges");
            }
            sampleReads.add(vertexId + "@" + depth);
            List<IEdge<Long, Integer>> sampled = DeterministicNeighborSampler.sample(
                vertexId, adjacency.get(vertexId), spec.getDirection(), spec.getFanout(),
                Long::compare, spec.getMaxReturnedEdges(), spec.getSeed(), SAMPLING_VERSION,
                SubgraphSamplingEndToEndTest::longBytes);
            return neighborhood(vertexId, sampled);
        }

        private LocalNeighborhood<Long, Integer, Integer> vertexOnly(long vertexId, int depth) {
            vertexOnlyReads.add(vertexId + "@" + depth);
            return neighborhood(vertexId, Collections.emptyList());
        }

        private LocalNeighborhood<Long, Integer, Integer> neighborhood(
            long vertexId, List<IEdge<Long, Integer>> edges) {
            return new LocalNeighborhood<>(new ValueVertex<>(vertexId, (int) vertexId),
                edges, SNAPSHOT_VERSION, SAMPLING_VERSION);
        }

        private List<String> getSampleReads() {
            return sampleReads;
        }

        private List<String> getVertexOnlyReads() {
            return vertexOnlyReads;
        }
    }
}
