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
import org.apache.geaflow.api.graph.function.vc.VertexCentricTraversalFunction.TraversalEdgeQuery;
import org.apache.geaflow.api.graph.function.vc.VertexCentricTraversalFunction.VertexCentricTraversalFuncContext;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.common.exception.GeaflowRuntimeException;
import org.apache.geaflow.common.iterator.CloseableIterator;
import org.apache.geaflow.dsl.common.algo.AlgorithmRuntimeContext;
import org.apache.geaflow.dsl.common.data.Row;
import org.apache.geaflow.dsl.common.data.RowEdge;
import org.apache.geaflow.dsl.common.exception.GeaFlowDSLException;
import org.apache.geaflow.dsl.common.types.GraphSchema;
import org.apache.geaflow.dsl.runtime.traversal.message.ITraversalAgg;
import org.apache.geaflow.model.graph.edge.EdgeDirection;
import org.apache.geaflow.model.traversal.ITraversalResponse;
import org.apache.geaflow.model.traversal.TraversalType.ResponseType;
import org.apache.geaflow.state.pushdown.filter.EmptyFilter;
import org.apache.geaflow.state.pushdown.filter.IFilter;
import org.apache.geaflow.state.pushdown.filter.InEdgeFilter;
import org.apache.geaflow.state.pushdown.filter.OutEdgeFilter;

public class GeaFlowAlgorithmRuntimeContext implements AlgorithmRuntimeContext<Object, Object> {

    private final VertexCentricTraversalFuncContext<Object, Row, Row, Object, Row> traversalContext;

    protected VertexCentricAggContext<ITraversalAgg, ITraversalAgg> aggContext;

    private final GraphSchema graphSchema;
    private final TraversalEdgeQuery<Object, Row> edgeQuery;
    private final transient GeaFlowAlgorithmAggTraversalFunction traversalFunction;
    private Object vertexId;

    private long lastSendAggMsgIterationId = -1L;

    public GeaFlowAlgorithmRuntimeContext(
        GeaFlowAlgorithmAggTraversalFunction traversalFunction,
        VertexCentricTraversalFuncContext<Object, Row, Row, Object, Row> traversalContext,
        GraphSchema graphSchema) {
        this.traversalFunction = traversalFunction;
        this.traversalContext = traversalContext;
        this.edgeQuery = traversalContext.edges();
        this.graphSchema = graphSchema;
        this.aggContext = null;
    }

    public void setVertexId(Object vertexId) {
        this.vertexId = vertexId;
        this.edgeQuery.withId(vertexId);
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

    @Override
    public List<RowEdge> loadStaticEdges(EdgeDirection direction) {
        return loadEdges(direction);
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
    public List<RowEdge> loadDynamicEdges(EdgeDirection direction) {
        throw new GeaflowRuntimeException("GeaFlowAlgorithmRuntimeContext not support loadDynamicEdges");
    }

    @Override
    public CloseableIterator<RowEdge> loadDynamicEdgesIterator(EdgeDirection direction) {
        throw new GeaflowRuntimeException("GeaFlowAlgorithmRuntimeContext not support loadDynamicEdgesIterator");
    }

    @Override
    public CloseableIterator<RowEdge> loadDynamicEdgesIterator(IFilter filter) {
        throw new GeaflowRuntimeException("GeaFlowAlgorithmRuntimeContext not support loadDynamicEdgesIterator");
    }

    @Override
    public void sendMessage(Object vertexId, Object message) {
        traversalContext.sendMessage(vertexId, message);
        if (getCurrentIterationId() > lastSendAggMsgIterationId) {
            lastSendAggMsgIterationId = getCurrentIterationId();
            aggContext.aggregate(GeaFlowKVAlgorithmAggregateFunction.getAlgorithmAgg(
                lastSendAggMsgIterationId));
        }
    }

    @Override
    public void updateVertexValue(Row value) {
        traversalFunction.updateVertexValue(vertexId, value);
    }

    @Override
    public void take(Row row) {
        traversalContext.takeResponse(new AlgorithmResponse(row));
    }

    public void finish() {

    }

    public void close() {

    }

    public long getCurrentIterationId() {
        return traversalContext.getIterationId();
    }

    @Override
    public Configuration getConfig() {
        return traversalContext.getRuntimeContext().getConfiguration();
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
