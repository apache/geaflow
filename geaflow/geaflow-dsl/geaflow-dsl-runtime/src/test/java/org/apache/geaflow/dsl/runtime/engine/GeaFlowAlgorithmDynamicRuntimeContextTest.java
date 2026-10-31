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

package org.apache.geaflow.dsl.runtime.engine;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;
import org.apache.geaflow.api.context.RuntimeContext;
import org.apache.geaflow.api.graph.sampling.SubgraphSamplingSpec;
import org.apache.geaflow.api.graph.function.vc.IncVertexCentricTraversalFunction.IncVertexCentricTraversalFuncContext;
import org.apache.geaflow.api.graph.function.vc.IncVertexCentricTraversalFunction.TraversalGraphSnapShot;
import org.apache.geaflow.api.graph.function.vc.IncVertexCentricTraversalFunction.TraversalHistoricalGraph;
import org.apache.geaflow.api.graph.function.vc.VertexCentricTraversalFunction.TraversalEdgeQuery;
import org.apache.geaflow.api.graph.function.vc.VertexCentricTraversalFunction.TraversalVertexQuery;
import org.apache.geaflow.api.graph.function.vc.base.IncVertexCentricFunction.TemporaryGraph;
import org.apache.geaflow.common.iterator.CloseableIterator;
import org.apache.geaflow.common.type.primitive.LongType;
import org.apache.geaflow.dsl.common.algo.AlgorithmUserFunction;
import org.apache.geaflow.dsl.common.data.Row;
import org.apache.geaflow.dsl.common.data.RowEdge;
import org.apache.geaflow.dsl.common.data.RowVertex;
import org.apache.geaflow.dsl.common.types.GraphSchema;
import org.apache.geaflow.dsl.common.data.impl.types.ObjectEdge;
import org.apache.geaflow.dsl.common.data.impl.ObjectRow;
import org.apache.geaflow.model.graph.edge.EdgeDirection;
import org.apache.geaflow.model.graph.edge.IEdge;
import org.apache.geaflow.state.pushdown.filter.OutEdgeFilter;
import org.apache.geaflow.state.sampling.LocalNeighborhood;
import org.testng.Assert;
import org.testng.annotations.Test;

public class GeaFlowAlgorithmDynamicRuntimeContextTest {

    @Test
    public void testSamplingUsesMaterializedSnapshotOnly() {
        IncVertexCentricTraversalFuncContext<Object, Row, Row, Object, Row> traversalContext = mock(
            IncVertexCentricTraversalFuncContext.class);
        TraversalHistoricalGraph<Object, Row, Row> historicalGraph = mock(TraversalHistoricalGraph.class);
        TraversalGraphSnapShot<Object, Row, Row> snapshot = mock(TraversalGraphSnapShot.class);
        TraversalVertexQuery<Object, Row> vertexQuery = mock(TraversalVertexQuery.class);
        TraversalEdgeQuery<Object, Row> edgeQuery = mock(TraversalEdgeQuery.class);
        RuntimeContext runtimeContext = mock(RuntimeContext.class);
        TemporaryGraph<Object, Row, Row> temporaryGraph = mock(TemporaryGraph.class);
        CloseableIterator<IEdge<Object, Row>> edgeIterator = mock(CloseableIterator.class);
        GraphSchema graphSchema = mock(GraphSchema.class);

        RowEdge edge = new ObjectEdge(1L, 2L, ObjectRow.create(1.0D));
        edge.setDirect(EdgeDirection.OUT);
        when(edgeIterator.hasNext()).thenReturn(true, false);
        when(edgeIterator.next()).thenReturn(edge);
        when(traversalContext.getHistoricalGraph()).thenReturn(historicalGraph);
        when(historicalGraph.getSnapShot(0L)).thenReturn(snapshot);
        when(snapshot.vertex()).thenReturn(vertexQuery);
        when(snapshot.edges()).thenReturn(edgeQuery);
        when(edgeQuery.getOutEdges()).thenReturn(Collections.singletonList(edge));
        when(edgeQuery.getEdges(OutEdgeFilter.getInstance())).thenReturn(edgeIterator);
        when(traversalContext.getRuntimeContext()).thenReturn(runtimeContext);
        when(runtimeContext.getWindowId()).thenReturn(7L);
        when(traversalContext.getTemporaryGraph()).thenReturn(temporaryGraph);
        when(temporaryGraph.getEdges()).thenReturn(Arrays.asList(edge));
        doReturn(LongType.INSTANCE).when(graphSchema).getIdType();

        GeaFlowAlgorithmDynamicRuntimeContext context = new GeaFlowAlgorithmDynamicRuntimeContext(
            new GeaFlowAlgorithmDynamicAggTraversalFunction(graphSchema,
                mock(AlgorithmUserFunction.class), new Object[0]), traversalContext, graphSchema);
        RowVertex vertex = mock(RowVertex.class);
        when(vertex.getId()).thenReturn(1L);

        LocalNeighborhood<Object, Row, Row> neighborhood = context.sampleOneHop(vertex, EdgeDirection.OUT, -1);

        Assert.assertEquals(neighborhood.getEdges().size(), 1);
        Assert.assertEquals(neighborhood.getSnapshotVersion(), 7L);
        verify(temporaryGraph, never()).getEdges();
    }

    @Test
    public void testSamplingSpecPropagatesVersionAndClosesStaticIterator() {
        IncVertexCentricTraversalFuncContext<Object, Row, Row, Object, Row> traversalContext = mock(
            IncVertexCentricTraversalFuncContext.class);
        TraversalHistoricalGraph<Object, Row, Row> historicalGraph = mock(TraversalHistoricalGraph.class);
        TraversalGraphSnapShot<Object, Row, Row> snapshot = mock(TraversalGraphSnapShot.class);
        TraversalVertexQuery<Object, Row> vertexQuery = mock(TraversalVertexQuery.class);
        TraversalEdgeQuery<Object, Row> edgeQuery = mock(TraversalEdgeQuery.class);
        RuntimeContext runtimeContext = mock(RuntimeContext.class);
        CloseableIterator<IEdge<Object, Row>> edgeIterator = mock(CloseableIterator.class);
        GraphSchema graphSchema = mock(GraphSchema.class);

        RowEdge first = edge(1L, 2L);
        RowEdge second = edge(1L, 3L);
        RowEdge third = edge(1L, 4L);
        when(edgeIterator.hasNext()).thenReturn(true, true, true, false);
        when(edgeIterator.next()).thenReturn(first, second, third);
        when(traversalContext.getHistoricalGraph()).thenReturn(historicalGraph);
        when(historicalGraph.getSnapShot(0L)).thenReturn(snapshot);
        when(snapshot.vertex()).thenReturn(vertexQuery);
        when(snapshot.edges()).thenReturn(edgeQuery);
        when(edgeQuery.getEdges(OutEdgeFilter.getInstance())).thenReturn(edgeIterator);
        when(traversalContext.getRuntimeContext()).thenReturn(runtimeContext);
        when(runtimeContext.getWindowId()).thenReturn(7L);
        doReturn(LongType.INSTANCE).when(graphSchema).getIdType();

        GeaFlowAlgorithmDynamicRuntimeContext context = new GeaFlowAlgorithmDynamicRuntimeContext(
            new GeaFlowAlgorithmDynamicAggTraversalFunction(graphSchema,
                mock(AlgorithmUserFunction.class), new Object[0]), traversalContext, graphSchema);
        RowVertex vertex = mock(RowVertex.class);
        when(vertex.getId()).thenReturn(1L);

        LocalNeighborhood<Object, Row, Row> neighborhood = context.sampleOneHop(vertex,
            new SubgraphSamplingSpec(1, 1, EdgeDirection.OUT, 100L, 17L), 9L);

        Assert.assertEquals(neighborhood.getEdges().size(), 1);
        Assert.assertEquals(neighborhood.getSnapshotVersion(), 7L);
        Assert.assertEquals(neighborhood.getSamplingVersion(), 9L);
        verify(edgeIterator).close();
    }

    private RowEdge edge(long source, long target) {
        RowEdge edge = new ObjectEdge(source, target, ObjectRow.create(1.0D));
        edge.setDirect(EdgeDirection.OUT);
        return edge;
    }
}
