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

import java.util.Collections;
import java.util.List;
import org.apache.geaflow.model.graph.edge.EdgeDirection;
import org.apache.geaflow.model.graph.edge.impl.ValueEdge;
import org.apache.geaflow.model.graph.edge.impl.ValueLabelEdge;
import org.apache.geaflow.model.graph.edge.impl.ValueLabelTimeEdge;
import org.apache.geaflow.model.graph.vertex.impl.ValueVertex;
import org.apache.geaflow.state.sampling.LocalNeighborhood;
import org.testng.Assert;
import org.testng.annotations.Test;

public class LayeredSubgraphAssemblerTest {

    @Test
    public void testAssemblesOneHopPerLayerAndReleasesResult() {
        LayeredSubgraphAssembler<Long, Integer, Integer> assembler =
            new LayeredSubgraphAssembler<>();
        LocalNeighborhood<Long, Integer, Integer> root = neighborhood(1L, 2L, 7L);
        LocalNeighborhood<Long, Integer, Integer> firstHop = neighborhood(2L, 3L, 7L);
        LocalNeighborhood<Long, Integer, Integer> frontier = neighborhood(3L, 4L, 7L);

        assembler.start(1L, 2, root);
        Assert.assertTrue(assembler.registerRequest(1L, 2L, 1));
        assembler.add(new SubgraphSamplingResponse<>(1L, 1, firstHop));
        Assert.assertTrue(assembler.registerRequest(1L, 3L, 2));
        assembler.add(new SubgraphSamplingResponse<>(1L, 2, frontier));

        SampledSubgraph<Long, Integer, Integer> subgraph = assembler.take(1L);
        Assert.assertEquals(subgraph.getVertices().size(), 3);
        Assert.assertEquals(subgraph.getVertices().get(3L).getValue(), Integer.valueOf(3));
        Assert.assertFalse(subgraph.getVertices().containsKey(4L));
        Assert.assertEquals(subgraph.getEdgeLayers().size(), 2);
        Assert.assertEquals(subgraph.getEdgeLayers().get(0).get(0).getTargetId(), Long.valueOf(2L));
        Assert.assertEquals(subgraph.getEdgeLayers().get(1).get(0).getTargetId(), Long.valueOf(3L));
        Assert.assertTrue(subgraph.getEdgeLayers().stream()
            .flatMap(List::stream)
            .noneMatch(edge -> Long.valueOf(4L).equals(edge.getTargetId())));
        Assert.assertNull(assembler.take(1L));
    }

    @Test
    public void testAcceptsOutOfOrderResponsesAndIgnoresDuplicates() {
        LayeredSubgraphAssembler<Long, Integer, Integer> assembler =
            new LayeredSubgraphAssembler<>();
        assembler.start(1L, 1, new LocalNeighborhood<>(new ValueVertex<>(1L, 1),
            java.util.Arrays.asList(new ValueEdge<>(1L, 2L, 1, EdgeDirection.OUT),
                new ValueEdge<>(1L, 3L, 1, EdgeDirection.OUT)), 7L));
        Assert.assertTrue(assembler.registerRequest(1L, 2L, 1));
        Assert.assertTrue(assembler.registerRequest(1L, 3L, 1));

        SubgraphSamplingResponse<Long, Integer, Integer> responseForThree =
            new SubgraphSamplingResponse<>(1L, 1, neighborhood(3L, 4L, 7L));
        SubgraphSamplingResponse<Long, Integer, Integer> responseForTwo =
            new SubgraphSamplingResponse<>(1L, 1, neighborhood(2L, 5L, 7L));
        Assert.assertTrue(assembler.add(responseForThree));
        Assert.assertTrue(assembler.add(responseForTwo));
        Assert.assertFalse(assembler.add(responseForThree));

        Assert.assertEquals(assembler.take(1L).getVertices().keySet(),
            new java.util.LinkedHashSet<>(java.util.Arrays.asList(1L, 2L, 3L)));
    }

    @Test
    public void testKeepsAssembliesIsolatedAndClearRemovesThem() {
        LayeredSubgraphAssembler<Long, Integer, Integer> assembler =
            new LayeredSubgraphAssembler<>();
        assembler.start(1L, 1, neighborhood(1L, 2L, 7L));
        assembler.start(10L, 1, neighborhood(10L, 11L, 7L));

        Assert.assertTrue(assembler.registerRequest(1L, 2L, 1));
        Assert.assertTrue(assembler.registerRequest(10L, 11L, 1));
        Assert.assertTrue(assembler.add(new SubgraphSamplingResponse<>(1L, 1,
            neighborhood(2L, 3L, 7L))));
        Assert.assertTrue(assembler.add(new SubgraphSamplingResponse<>(10L, 1,
            neighborhood(11L, 12L, 7L))));
        Assert.assertEquals(assembler.take(1L).getRootId(), Long.valueOf(1L));
        Assert.assertEquals(assembler.take(10L).getRootId(), Long.valueOf(10L));

        assembler.start(20L, 1, neighborhood(20L, 21L, 7L));
        assembler.clear();
        Assert.assertNull(assembler.take(20L));
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testRejectsRequestDepthOutsideAssembly() {
        LayeredSubgraphAssembler<Long, Integer, Integer> assembler =
            new LayeredSubgraphAssembler<>();
        assembler.start(1L, 1, neighborhood(1L, 2L, 7L));
        assembler.registerRequest(1L, 2L, 2);
    }

    @Test(expectedExceptions = IllegalStateException.class)
    public void testRejectsResponseForUnrequestedVertex() {
        LayeredSubgraphAssembler<Long, Integer, Integer> assembler =
            new LayeredSubgraphAssembler<>();
        assembler.start(1L, 1, neighborhood(1L, 2L, 7L));
        assembler.add(new SubgraphSamplingResponse<>(1L, 1, neighborhood(3L, 4L, 7L)));
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testRejectsNeighborhoodFromPreviousWindow() {
        LayeredSubgraphAssembler<Long, Integer, Integer> assembler =
            new LayeredSubgraphAssembler<>();
        assembler.start(1L, 1, neighborhood(1L, 2L, 7L));
        assembler.registerRequest(1L, 2L, 1);
        assembler.add(new SubgraphSamplingResponse<>(1L, 1, neighborhood(2L, 3L, 6L)));
    }

    @Test
    public void testDoesNotDuplicateLogicalEdgeAcrossLayers() {
        LayeredSubgraphAssembler<Long, Integer, Integer> assembler =
            new LayeredSubgraphAssembler<>();
        assembler.start(1L, 2, neighborhood(1L, 2L, 7L));
        assembler.registerRequest(1L, 2L, 1);
        assembler.add(new SubgraphSamplingResponse<>(1L, 1,
            neighborhoodWithEdge(2L, 1L, 2L, 7L)));

        SampledSubgraph<Long, Integer, Integer> subgraph = assembler.take(1L);
        long edgeCount = subgraph.getEdgeLayers().stream().mapToLong(List::size).sum();
        Assert.assertEquals(edgeCount, 1L);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testRejectsNeighborhoodFromFutureWindow() {
        LayeredSubgraphAssembler<Long, Integer, Integer> assembler =
            new LayeredSubgraphAssembler<>();
        assembler.start(1L, 1, neighborhood(1L, 2L, 7L));
        assembler.registerRequest(1L, 2L, 1);
        assembler.add(new SubgraphSamplingResponse<>(1L, 1, neighborhood(2L, 3L, 8L)));
    }

    @Test
    public void testCycleRegistersEachVertexOnlyOnce() {
        LayeredSubgraphAssembler<Long, Integer, Integer> assembler =
            new LayeredSubgraphAssembler<>();
        assembler.start(1L, 3, neighborhood(1L, 2L, 7L));

        Assert.assertTrue(assembler.registerRequest(1L, 2L, 1));
        Assert.assertFalse(assembler.registerRequest(1L, 1L, 2));
        Assert.assertFalse(assembler.registerRequest(1L, 2L, 2));
        assembler.add(new SubgraphSamplingResponse<>(1L, 1,
            neighborhoodWithEdge(2L, 2L, 1L, 7L)));

        Assert.assertEquals(assembler.take(1L).getVertices().size(), 2);
    }

    @Test(expectedExceptions = SubgraphSamplingLimitException.class)
    public void testRejectsNodeBudgetOverflow() {
        LayeredSubgraphAssembler<Long, Integer, Integer> assembler =
            new LayeredSubgraphAssembler<>(1, 10, Long::compare);
        assembler.start(1L, 1, neighborhood(1L, 2L, 7L));
        assembler.registerRequest(1L, 2L, 1);
    }

    @Test(expectedExceptions = SubgraphSamplingLimitException.class)
    public void testRejectsEdgeBudgetOverflow() {
        LayeredSubgraphAssembler<Long, Integer, Integer> assembler =
            new LayeredSubgraphAssembler<>(10, 1, Long::compare);
        LocalNeighborhood<Long, Integer, Integer> root = new LocalNeighborhood<>(
            new ValueVertex<>(1L, 1), java.util.Arrays.asList(
            new ValueEdge<>(1L, 2L, 1, EdgeDirection.OUT),
            new ValueEdge<>(1L, 3L, 1, EdgeDirection.OUT)), 7L);
        assembler.start(1L, 1, root);
    }

    @Test(expectedExceptions = IllegalStateException.class)
    public void testRejectsMissingResponse() {
        LayeredSubgraphAssembler<Long, Integer, Integer> assembler =
            new LayeredSubgraphAssembler<>();
        assembler.start(1L, 1, neighborhood(1L, 2L, 7L));
        assembler.registerRequest(1L, 2L, 1);
        assembler.take(1L);
    }

    @Test
    public void testStructuralEdgeIdentityPreservesCollisionsAndLabels() {
        SampledSubgraph<String, Integer, Integer> subgraph = new SampledSubgraph<>("root", 1L);
        LocalNeighborhood<String, Integer, Integer> neighborhood = new LocalNeighborhood<>(
            new ValueVertex<>("root", 0), java.util.Arrays.asList(
            new ValueLabelEdge<>("a->b", "c", 1, "first"),
            new ValueLabelEdge<>("a", "b->c", 1, "first"),
            new ValueLabelEdge<>("a", "b->c", 1, "second")), 1L);
        subgraph.addNeighborhood(0, neighborhood, true);

        Assert.assertEquals(subgraph.getEdgeLayers().get(0).size(), 3);
    }

    @Test
    public void testLogicalEdgeIdentityIgnoresReplicaDirectionAndValue() {
        ValueLabelEdge<Long, String> out = new ValueLabelEdge<>(1L, 2L, "first", "knows");
        out.setDirect(EdgeDirection.OUT);
        ValueLabelEdge<Long, String> inReplica = new ValueLabelEdge<>(1L, 2L, "second", "knows");
        inReplica.setDirect(EdgeDirection.IN);
        ValueLabelEdge<Long, String> reciprocal = new ValueLabelEdge<>(2L, 1L, "third", "knows");

        SampledSubgraph<Long, Integer, String> subgraph = new SampledSubgraph<>(1L, 1L);
        subgraph.addNeighborhood(0, new LocalNeighborhood<>(new ValueVertex<>(1L, 1),
            java.util.Arrays.asList(out, inReplica, reciprocal), 1L), true);

        Assert.assertEquals(subgraph.getEdgeLayers().get(0).size(), 2);
    }

    @Test
    public void testLogicalEdgeIdentityPreservesTemporalParallelEdges() {
        SampledSubgraph<Long, Integer, String> subgraph = new SampledSubgraph<>(1L, 1L);
        subgraph.addNeighborhood(0, new LocalNeighborhood<>(new ValueVertex<>(1L, 1),
            java.util.Arrays.asList(
            new ValueLabelTimeEdge<>(1L, 2L, "same", "knows", 10L),
            new ValueLabelTimeEdge<>(1L, 2L, "same", "knows", 11L)), 1L), true);

        Assert.assertEquals(subgraph.getEdgeLayers().get(0).size(), 2);
    }

    private LocalNeighborhood<Long, Integer, Integer> neighborhood(long source, long target,
                                                                   long version) {
        return neighborhoodWithEdge(source, source, target, version);
    }

    private LocalNeighborhood<Long, Integer, Integer> neighborhoodWithEdge(long vertexId,
                                                                            long source,
                                                                            long target,
                                                                            long version) {
        ValueVertex<Long, Integer> vertex = new ValueVertex<>(vertexId, (int) vertexId);
        ValueEdge<Long, Integer> edge = new ValueEdge<>(source, target, 1, EdgeDirection.OUT);
        return new LocalNeighborhood<>(vertex, Collections.singletonList(edge), version);
    }
}
