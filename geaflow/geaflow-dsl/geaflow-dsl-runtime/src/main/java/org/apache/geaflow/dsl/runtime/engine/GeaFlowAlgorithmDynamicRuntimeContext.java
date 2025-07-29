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

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.apache.geaflow.api.graph.function.aggregate.VertexCentricAggContextFunction.VertexCentricAggContext;
import org.apache.geaflow.api.graph.function.vc.IncVertexCentricTraversalFunction.IncVertexCentricTraversalFuncContext;
import org.apache.geaflow.api.graph.function.vc.IncVertexCentricTraversalFunction.TraversalGraphSnapShot;
import org.apache.geaflow.api.graph.function.vc.VertexCentricTraversalFunction.TraversalEdgeQuery;
import org.apache.geaflow.api.graph.function.vc.VertexCentricTraversalFunction.TraversalVertexQuery;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.common.iterator.CloseableIterator;
import org.apache.geaflow.dsl.common.algo.AlgorithmRuntimeContext;
import org.apache.geaflow.dsl.common.data.Row;
import org.apache.geaflow.dsl.common.data.RowEdge;
import org.apache.geaflow.dsl.common.exception.GeaFlowDSLException;
import org.apache.geaflow.dsl.common.types.GraphSchema;
import org.apache.geaflow.dsl.runtime.traversal.message.ITraversalAgg;
import org.apache.geaflow.model.graph.edge.EdgeDirection;
import org.apache.geaflow.model.graph.edge.IEdge;
import org.apache.geaflow.model.graph.vertex.IVertex;
import org.apache.geaflow.model.traversal.ITraversalResponse;
import org.apache.geaflow.model.traversal.TraversalType.ResponseType;
import org.apache.geaflow.state.iterator.IteratorWithClose;
import org.apache.geaflow.state.pushdown.filter.EmptyFilter;
import org.apache.geaflow.state.pushdown.filter.IFilter;
import org.apache.geaflow.state.pushdown.filter.InEdgeFilter;
import org.apache.geaflow.state.pushdown.filter.OutEdgeFilter;

public class GeaFlowAlgorithmDynamicRuntimeContext implements AlgorithmRuntimeContext<Object, Object> {

    private final IncVertexCentricTraversalFuncContext<Object, Row, Row, Object, Row> incVCTraversalCtx;

    protected VertexCentricAggContext<ITraversalAgg, ITraversalAgg> aggContext;

    private final GraphSchema graphSchema;

    protected TraversalVertexQuery<Object, Row> vertexQuery;

    protected TraversalEdgeQuery<Object, Row> edgeQuery;

    private final transient GeaFlowAlgorithmDynamicAggTraversalFunction traversalFunction;

    private Object vertexId;

    private long iterationId = -1L;

    public GeaFlowAlgorithmDynamicRuntimeContext(GeaFlowAlgorithmDynamicAggTraversalFunction traversalFunction,
                                                 IncVertexCentricTraversalFuncContext<Object, Row, Row, Object, Row> traversalContext, GraphSchema graphSchema) {
        this.traversalFunction = traversalFunction;
        this.incVCTraversalCtx = traversalContext;
        this.graphSchema = graphSchema;
        TraversalGraphSnapShot<Object, Row, Row> graphSnapShot = incVCTraversalCtx.getHistoricalGraph().getSnapShot(0L);
        this.vertexQuery = graphSnapShot.vertex();
        this.edgeQuery = graphSnapShot.edges();
    }

    public void setVertexId(Object vertexId) {
        this.vertexId = vertexId;
        this.vertexQuery.withId(vertexId);
        this.edgeQuery.withId(vertexId);
    }

    public IVertex loadVertex() {
        return vertexQuery.get();
    }

    public CloseableIterator<Object> loadAllVertex() {
        return vertexQuery.loadIdIterator();
    }

    @Override
    public Configuration getConfig() {
        return incVCTraversalCtx.getRuntimeContext().getConfiguration();
    }

    @SuppressWarnings("unchecked")
    @Override
    public List<RowEdge> loadEdges(EdgeDirection direction) {
        switch (direction) {
            case OUT:
                return (List) edgeQuery.getOutEdges();
            case IN:
                return (List) edgeQuery.getInEdges();
            case BOTH:
                List<RowEdge> edges = new ArrayList<>();
                edges.addAll((List) edgeQuery.getOutEdges());
                edges.addAll((List) edgeQuery.getInEdges());
                return edges;
            default:
                throw new GeaFlowDSLException("Illegal edge direction: " + direction);
        }
    }

    @Override
    public CloseableIterator<RowEdge> loadEdgesIterator(EdgeDirection direction) {
        switch (direction) {
            case OUT:
                return loadEdgesIterator(OutEdgeFilter.getInstance());
            case IN:
                return loadEdgesIterator(InEdgeFilter.getInstance());
            case BOTH:
                return loadEdgesIterator(EmptyFilter.getInstance());
            default:
                throw new GeaFlowDSLException("Illegal edge direction: " + direction);
        }
    }

    @Override
    public CloseableIterator<RowEdge> loadEdgesIterator(IFilter filter) {
        return (CloseableIterator) edgeQuery.getEdges(filter);
    }

    public List<RowEdge> loadDynamicEdges(EdgeDirection direction) {
        List<IEdge<Object, Row>> edges = incVCTraversalCtx.getTemporaryGraph().getEdges();
        List<RowEdge> rowEdges = new ArrayList<>();
        if (edges == null) {
            return rowEdges;
        }
        switch (direction) {
            case OUT:
                for (IEdge<Object, Row> edge : edges) {
                    if (edge.getDirect() == EdgeDirection.OUT) {
                        rowEdges.add((RowEdge) edge);
                    }
                }
                break;
            case IN:
                for (IEdge<Object, Row> edge : edges) {
                    if (edge.getDirect() == EdgeDirection.IN) {
                        rowEdges.add((RowEdge) edge);
                    }
                }
                break;
            case BOTH:
                for (IEdge<Object, Row> edge : edges) {
                    rowEdges.add((RowEdge) edge);
                }
                break;
            default:
                throw new GeaFlowDSLException("Illegal edge direction: " + direction);
        }
        return rowEdges;
    }

    public List<RowEdge> loadStaticEdges(EdgeDirection direction) {
        switch (direction) {
            case OUT:
                return (List) edgeQuery.getOutEdges();
            case IN:
                return (List) edgeQuery.getInEdges();
            case BOTH:
                return (List) edgeQuery.getEdges();
            default:
                throw new GeaFlowDSLException("Illegal edge direction: " + direction);
        }
    }

    @Override
    public CloseableIterator<RowEdge> loadStaticEdgesIterator(EdgeDirection direction) {
        switch (direction) {
            case OUT:
                return loadStaticEdgesIterator(OutEdgeFilter.getInstance());
            case IN:
                return loadStaticEdgesIterator(InEdgeFilter.getInstance());
            case BOTH:
                return loadStaticEdgesIterator(EmptyFilter.getInstance());
            default:
                throw new GeaFlowDSLException("Illegal edge direction: " + direction);
        }
    }

    @Override
    public CloseableIterator<RowEdge> loadStaticEdgesIterator(IFilter filter) {
        return (CloseableIterator) edgeQuery.getEdges(filter);
    }

    @Override
    public CloseableIterator<RowEdge> loadDynamicEdgesIterator(EdgeDirection direction) {
        switch (direction) {
            case OUT:
                return loadDynamicEdgesIterator(OutEdgeFilter.getInstance());
            case IN:
                return loadDynamicEdgesIterator(InEdgeFilter.getInstance());
            case BOTH:
                return loadDynamicEdgesIterator(EmptyFilter.getInstance());
            default:
                throw new GeaFlowDSLException("Illegal edge direction: " + direction);
        }
    }

    @Override
    public CloseableIterator<RowEdge> loadDynamicEdgesIterator(IFilter filter) {
        List<IEdge<Object, Row>> edges = incVCTraversalCtx.getTemporaryGraph().getEdges();
        List<RowEdge> res = new ArrayList<>();
        for (IEdge<Object, Row> edge : edges) {
            if (filter.filter(edge)) {
                res.add((RowEdge) edge);
            }
        }
        return IteratorWithClose.wrap(res.iterator());
    }

    @Override
    public void sendMessage(Object vertexId, Object message) {
        incVCTraversalCtx.sendMessage(vertexId, message);
        if (getCurrentIterationId() > iterationId) {
            iterationId = getCurrentIterationId();
            aggContext.aggregate(GeaFlowKVAlgorithmAggregateFunction.getAlgorithmAgg(iterationId));
        }
    }

    @Override
    public void take(Row row) {
        incVCTraversalCtx.takeResponse(new AlgorithmResponse(row));
    }

    @Override
    public void updateVertexValue(Row value) {
        traversalFunction.updateVertexValue(vertexId, value);
    }

    public long getCurrentIterationId() {
        return incVCTraversalCtx.getIterationId();
    }

    public void finish() {

    }

    public void close() {

    }

    @Override
    public GraphSchema getGraphSchema() {
        return graphSchema;
    }

    public VertexCentricAggContext<ITraversalAgg, ITraversalAgg> getAggContext() {
        return aggContext;
    }

    public void setAggContext(VertexCentricAggContext<ITraversalAgg, ITraversalAgg> aggContext) {
        this.aggContext = Objects.requireNonNull(aggContext);
    }

    public IncVertexCentricTraversalFuncContext<Object, Row, Row, Object, Row> getIncVCTraversalCtx() {
        return incVCTraversalCtx;
    }

    private static class AlgorithmResponse implements ITraversalResponse<Row> {

        private final Row row;

        public AlgorithmResponse(Row row) {
            this.row = row;
        }

        @Override
        public long getResponseId() {
            return 0;
        }

        @Override
        public Row getResponse() {
            return row;
        }

        @Override
        public ResponseType getType() {
            return ResponseType.Vertex;
        }
    }
}
