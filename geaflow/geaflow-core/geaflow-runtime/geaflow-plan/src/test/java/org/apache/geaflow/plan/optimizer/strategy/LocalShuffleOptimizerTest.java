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

package org.apache.geaflow.plan.optimizer.strategy;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.geaflow.api.context.RuntimeContext;
import org.apache.geaflow.collector.ICollector;
import org.apache.geaflow.common.encoder.IEncoder;
import org.apache.geaflow.operator.Operator;
import org.apache.geaflow.partitioner.IPartitioner;
import org.apache.geaflow.partitioner.impl.ForwardPartitioner;
import org.apache.geaflow.partitioner.impl.KeyPartitioner;
import org.apache.geaflow.plan.graph.PipelineEdge;
import org.apache.geaflow.plan.graph.PipelineGraph;
import org.apache.geaflow.plan.graph.PipelineVertex;
import org.apache.geaflow.plan.graph.VertexType;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Unit tests for LocalShuffleOptimizer.
 *
 * <p>This test class verifies the local shuffle optimization logic which detects
 * graph operator → sink/map patterns and marks them for co-location to enable
 * automatic local shuffle through LocalInputChannel.
 */
public class LocalShuffleOptimizerTest {

    /**
     * Test basic optimization scenario: GraphTraversal → Sink with forward partition.
     *
     * <p>Expected behavior:
     * - Both vertices should be marked with the same coLocationGroup
     * - Optimization should succeed for matching parallelism and forward partition
     */
    @Test
    public void testGraphToSinkWithForwardPartition() {
        // Create mock pipeline graph
        PipelineGraph pipelineGraph = createMockPipelineGraph();

        // Create vertices
        PipelineVertex graphVertex = createGraphVertex(1, 4); // Graph operator, parallelism 4
        PipelineVertex sinkVertex = createSinkVertex(2, 4);   // Sink operator, parallelism 4

        // Add vertices to graph
        Map<Integer, PipelineVertex> vertexMap = new HashMap<>();
        vertexMap.put(1, graphVertex);
        vertexMap.put(2, sinkVertex);
        when(pipelineGraph.getVertexMap()).thenReturn(vertexMap);

        // Create edge with forward partition
        PipelineEdge edge = createEdge(1, 1, 2, new ForwardPartitioner<>());
        List<PipelineEdge> edges = new ArrayList<>();
        edges.add(edge);
        when(pipelineGraph.getPipelineEdgeList()).thenReturn(edges);

        // Mock single input for sink vertex
        mockSingleInput(pipelineGraph, 2, edge);

        // Apply optimization
        LocalShuffleOptimizer optimizer = new LocalShuffleOptimizer();
        optimizer.optimize(pipelineGraph);

        // Verify co-location
        Assert.assertNotNull(graphVertex.getCoLocationGroup(),
            "Graph vertex should have coLocationGroup");
        Assert.assertNotNull(sinkVertex.getCoLocationGroup(),
            "Sink vertex should have coLocationGroup");
        Assert.assertEquals(graphVertex.getCoLocationGroup(), sinkVertex.getCoLocationGroup(),
            "Both vertices should have the same coLocationGroup");
    }

    /**
     * Test chain scenario: GraphTraversal → Map → Sink.
     *
     * <p>Expected behavior:
     * - Graph → Map should be optimized (forward partition, single input, graph→sink/map pattern)
     * - Map → Sink should NOT be optimized (source is not graph operator)
     * - Graph and Map vertices should have co-location group
     * - Sink vertex may or may not have co-location (depends on whether map was marked)
     */
    @Test
    public void testGraphToMapToSinkChain() {
        // Create mock pipeline graph
        PipelineGraph pipelineGraph = createMockPipelineGraph();

        // Create vertices
        PipelineVertex graphVertex = createGraphVertex(1, 4);
        PipelineVertex mapVertex = createMapVertex(2, 4);
        PipelineVertex sinkVertex = createSinkVertex(3, 4);

        // Add vertices to graph
        Map<Integer, PipelineVertex> vertexMap = new HashMap<>();
        vertexMap.put(1, graphVertex);
        vertexMap.put(2, mapVertex);
        vertexMap.put(3, sinkVertex);
        when(pipelineGraph.getVertexMap()).thenReturn(vertexMap);

        // Create edges with forward partition
        PipelineEdge edge1 = createEdge(1, 1, 2, new ForwardPartitioner<>());
        PipelineEdge edge2 = createEdge(2, 2, 3, new ForwardPartitioner<>());
        List<PipelineEdge> edges = new ArrayList<>();
        edges.add(edge1);
        edges.add(edge2);
        when(pipelineGraph.getPipelineEdgeList()).thenReturn(edges);

        // Mock single inputs
        mockSingleInput(pipelineGraph, 2, edge1);
        mockSingleInput(pipelineGraph, 3, edge2);

        // Apply optimization
        LocalShuffleOptimizer optimizer = new LocalShuffleOptimizer();
        optimizer.optimize(pipelineGraph);

        // Verify co-location for graph → map (should be optimized)
        Assert.assertNotNull(graphVertex.getCoLocationGroup(),
            "Graph vertex should have coLocationGroup");
        Assert.assertNotNull(mapVertex.getCoLocationGroup(),
            "Map vertex should have coLocationGroup");
        Assert.assertEquals(graphVertex.getCoLocationGroup(), mapVertex.getCoLocationGroup(),
            "Graph and Map vertices should share same coLocationGroup");

        // Map → Sink should NOT be optimized (map is not a graph operator)
        // So Sink may not have a coLocationGroup, or if Map was marked by first edge,
        // Sink will not share it because second edge doesn't meet criteria
    }

    /**
     * Test negative case: Graph → Sink with KEY partition (should not optimize).
     *
     * <p>Expected behavior:
     * - Optimization should be skipped because partition type is not FORWARD
     * - Vertices should NOT have coLocationGroup
     */
    @Test
    public void testNoOptimizationForKeyPartition() {
        // Create mock pipeline graph
        PipelineGraph pipelineGraph = createMockPipelineGraph();

        // Create vertices
        PipelineVertex graphVertex = createGraphVertex(1, 4);
        PipelineVertex sinkVertex = createSinkVertex(2, 4);

        // Add vertices to graph
        Map<Integer, PipelineVertex> vertexMap = new HashMap<>();
        vertexMap.put(1, graphVertex);
        vertexMap.put(2, sinkVertex);
        when(pipelineGraph.getVertexMap()).thenReturn(vertexMap);

        // Create edge with KEY partition (not FORWARD)
        PipelineEdge edge = createEdge(1, 1, 2, new KeyPartitioner<>(1));
        List<PipelineEdge> edges = new ArrayList<>();
        edges.add(edge);
        when(pipelineGraph.getPipelineEdgeList()).thenReturn(edges);

        // Mock single input for sink vertex
        mockSingleInput(pipelineGraph, 2, edge);

        // Apply optimization
        LocalShuffleOptimizer optimizer = new LocalShuffleOptimizer();
        optimizer.optimize(pipelineGraph);

        // Verify NO co-location due to key partition
        Assert.assertNull(graphVertex.getCoLocationGroup(),
            "Graph vertex should NOT have coLocationGroup with key partition");
        Assert.assertNull(sinkVertex.getCoLocationGroup(),
            "Sink vertex should NOT have coLocationGroup with key partition");
    }

    /**
     * Test negative case: Sink with multiple inputs (should not optimize).
     *
     * <p>Expected behavior:
     * - Optimization should be skipped because sink has multiple inputs
     * - Vertices should NOT have coLocationGroup
     */
    @Test
    public void testSinkWithMultipleInputs() {
        // Create mock pipeline graph
        PipelineGraph pipelineGraph = createMockPipelineGraph();

        // Create vertices
        PipelineVertex graphVertex1 = createGraphVertex(1, 4);
        PipelineVertex graphVertex2 = createGraphVertex(2, 4);
        PipelineVertex sinkVertex = createSinkVertex(3, 4);

        // Add vertices to graph
        Map<Integer, PipelineVertex> vertexMap = new HashMap<>();
        vertexMap.put(1, graphVertex1);
        vertexMap.put(2, graphVertex2);
        vertexMap.put(3, sinkVertex);
        when(pipelineGraph.getVertexMap()).thenReturn(vertexMap);

        // Create edges - TWO inputs to sink
        PipelineEdge edge1 = createEdge(1, 1, 3, new ForwardPartitioner<>());
        PipelineEdge edge2 = createEdge(2, 2, 3, new ForwardPartitioner<>());
        List<PipelineEdge> edges = new ArrayList<>();
        edges.add(edge1);
        edges.add(edge2);
        when(pipelineGraph.getPipelineEdgeList()).thenReturn(edges);

        // Mock MULTIPLE inputs for sink vertex
        mockMultipleInputs(pipelineGraph, 3, edge1, edge2);

        // Apply optimization
        LocalShuffleOptimizer optimizer = new LocalShuffleOptimizer();
        optimizer.optimize(pipelineGraph);

        // Verify NO co-location due to multiple inputs
        Assert.assertNull(graphVertex1.getCoLocationGroup(),
            "Graph vertex 1 should NOT have coLocationGroup with multiple sink inputs");
        Assert.assertNull(graphVertex2.getCoLocationGroup(),
            "Graph vertex 2 should NOT have coLocationGroup with multiple sink inputs");
        Assert.assertNull(sinkVertex.getCoLocationGroup(),
            "Sink vertex should NOT have coLocationGroup with multiple inputs");
    }

    /**
     * Test negative case: Parallelism mismatch (should not optimize).
     *
     * <p>Expected behavior:
     * - Optimization should be skipped when parallelism doesn't match or divide evenly
     * - Example: 8 → 3 is not divisible, should fail
     */
    @Test
    public void testParallelismMismatch() {
        // Create mock pipeline graph
        PipelineGraph pipelineGraph = createMockPipelineGraph();

        // Create vertices with mismatched parallelism (8 → 3, not divisible)
        PipelineVertex graphVertex = createGraphVertex(1, 8);
        PipelineVertex sinkVertex = createSinkVertex(2, 3);

        // Add vertices to graph
        Map<Integer, PipelineVertex> vertexMap = new HashMap<>();
        vertexMap.put(1, graphVertex);
        vertexMap.put(2, sinkVertex);
        when(pipelineGraph.getVertexMap()).thenReturn(vertexMap);

        // Create edge with forward partition
        PipelineEdge edge = createEdge(1, 1, 2, new ForwardPartitioner<>());
        List<PipelineEdge> edges = new ArrayList<>();
        edges.add(edge);
        when(pipelineGraph.getPipelineEdgeList()).thenReturn(edges);

        // Mock single input for sink vertex
        mockSingleInput(pipelineGraph, 2, edge);

        // Apply optimization
        LocalShuffleOptimizer optimizer = new LocalShuffleOptimizer();
        optimizer.optimize(pipelineGraph);

        // Verify NO co-location due to parallelism mismatch
        Assert.assertNull(graphVertex.getCoLocationGroup(),
            "Graph vertex should NOT have coLocationGroup with parallelism mismatch");
        Assert.assertNull(sinkVertex.getCoLocationGroup(),
            "Sink vertex should NOT have coLocationGroup with parallelism mismatch");
    }

    /**
     * Test positive case: Compatible parallelism ratio (should optimize).
     *
     * <p>Expected behavior:
     * - Optimization should succeed for divisible parallelism ratios
     * - Example: 8 → 4 (2:1 ratio), 12 → 4 (3:1 ratio) should both succeed
     */
    @Test
    public void testCompatibleParallelismRatio() {
        // Test case 1: 8 → 4 (2:1 ratio)
        PipelineGraph pipelineGraph1 = createMockPipelineGraph();
        PipelineVertex graphVertex1 = createGraphVertex(1, 8);
        PipelineVertex sinkVertex1 = createSinkVertex(2, 4);

        Map<Integer, PipelineVertex> vertexMap1 = new HashMap<>();
        vertexMap1.put(1, graphVertex1);
        vertexMap1.put(2, sinkVertex1);
        when(pipelineGraph1.getVertexMap()).thenReturn(vertexMap1);

        PipelineEdge edge1 = createEdge(1, 1, 2, new ForwardPartitioner<>());
        List<PipelineEdge> edges1 = new ArrayList<>();
        edges1.add(edge1);
        when(pipelineGraph1.getPipelineEdgeList()).thenReturn(edges1);
        mockSingleInput(pipelineGraph1, 2, edge1);

        LocalShuffleOptimizer optimizer1 = new LocalShuffleOptimizer();
        optimizer1.optimize(pipelineGraph1);

        Assert.assertNotNull(graphVertex1.getCoLocationGroup(),
            "Graph vertex should have coLocationGroup with 8→4 parallelism");
        Assert.assertNotNull(sinkVertex1.getCoLocationGroup(),
            "Sink vertex should have coLocationGroup with 8→4 parallelism");

        // Test case 2: 12 → 4 (3:1 ratio)
        PipelineGraph pipelineGraph2 = createMockPipelineGraph();
        PipelineVertex graphVertex2 = createGraphVertex(3, 12);
        PipelineVertex sinkVertex2 = createSinkVertex(4, 4);

        Map<Integer, PipelineVertex> vertexMap2 = new HashMap<>();
        vertexMap2.put(3, graphVertex2);
        vertexMap2.put(4, sinkVertex2);
        when(pipelineGraph2.getVertexMap()).thenReturn(vertexMap2);

        PipelineEdge edge2 = createEdge(2, 3, 4, new ForwardPartitioner<>());
        List<PipelineEdge> edges2 = new ArrayList<>();
        edges2.add(edge2);
        when(pipelineGraph2.getPipelineEdgeList()).thenReturn(edges2);
        mockSingleInput(pipelineGraph2, 4, edge2);

        LocalShuffleOptimizer optimizer2 = new LocalShuffleOptimizer();
        optimizer2.optimize(pipelineGraph2);

        Assert.assertNotNull(graphVertex2.getCoLocationGroup(),
            "Graph vertex should have coLocationGroup with 12→4 parallelism");
        Assert.assertNotNull(sinkVertex2.getCoLocationGroup(),
            "Sink vertex should have coLocationGroup with 12→4 parallelism");
    }

    // ==================== Helper Methods ====================

    /**
     * Create a mock PipelineGraph.
     */
    private PipelineGraph createMockPipelineGraph() {
        return mock(PipelineGraph.class);
    }

    /**
     * Create a graph operator vertex.
     */
    private PipelineVertex createGraphVertex(int id, int parallelism) {
        Operator operator = new MockGraphTraversalOperator();
        PipelineVertex vertex = new PipelineVertex(id, operator, VertexType.source, parallelism);
        return vertex;
    }

    /**
     * Create a sink operator vertex.
     */
    private PipelineVertex createSinkVertex(int id, int parallelism) {
        Operator operator = new MockSinkOperator();
        PipelineVertex vertex = new PipelineVertex(id, operator, VertexType.sink, parallelism);
        return vertex;
    }

    /**
     * Create a map operator vertex.
     */
    private PipelineVertex createMapVertex(int id, int parallelism) {
        Operator operator = new MockMapOperator();
        PipelineVertex vertex = new PipelineVertex(id, operator, VertexType.process, parallelism);
        return vertex;
    }

    /**
     * Create a pipeline edge.
     */
    private PipelineEdge createEdge(int edgeId, int srcId, int targetId, IPartitioner partitioner) {
        IEncoder<?> encoder = mock(IEncoder.class);
        return new PipelineEdge(edgeId, srcId, targetId, partitioner, encoder);
    }

    /**
     * Mock single input for a vertex.
     */
    private void mockSingleInput(PipelineGraph graph, int vertexId, PipelineEdge edge) {
        Set<PipelineEdge> inputEdges = ImmutableSet.of(edge);
        when(graph.getVertexInputEdges(vertexId)).thenReturn(inputEdges);
    }

    /**
     * Mock multiple inputs for a vertex.
     */
    private void mockMultipleInputs(PipelineGraph graph, int vertexId, PipelineEdge... edges) {
        Set<PipelineEdge> inputEdges = ImmutableSet.copyOf(edges);
        when(graph.getVertexInputEdges(vertexId)).thenReturn(inputEdges);
    }

    // ==================== Mock Operator Classes ====================

    /**
     * Mock GraphTraversal operator (name contains "Graph" and "Traversal").
     */
    private static class MockGraphTraversalOperator implements Operator {

        @Override
        public void open(OpContext opContext) {
        }

        @Override
        public void finish() {
        }

        @Override
        public void close() {
        }
    }

    /**
     * Mock Sink operator (name contains "Sink").
     */
    private static class MockSinkOperator implements Operator {

        @Override
        public void open(OpContext opContext) {
        }

        @Override
        public void finish() {
        }

        @Override
        public void close() {
        }
    }

    /**
     * Mock Map operator (name contains "Map").
     */
    private static class MockMapOperator implements Operator {

        @Override
        public void open(OpContext opContext) {
        }

        @Override
        public void finish() {
        }

        @Override
        public void close() {
        }
    }
}
