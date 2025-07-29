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

import static org.apache.geaflow.operator.Constants.GRAPH_VERSION;

import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import org.apache.geaflow.api.function.iterator.RichIteratorFunction;
import org.apache.geaflow.api.graph.function.vc.IncVertexCentricAggTraversalFunction;
import org.apache.geaflow.dsl.common.data.Row;
import org.apache.geaflow.dsl.runtime.traversal.ExecuteDagGroup;
import org.apache.geaflow.dsl.runtime.traversal.message.ITraversalAgg;
import org.apache.geaflow.dsl.runtime.traversal.message.MessageBox;
import org.apache.geaflow.dsl.runtime.traversal.path.ITreePath;
import org.apache.geaflow.model.graph.edge.IEdge;
import org.apache.geaflow.model.graph.vertex.IVertex;
import org.apache.geaflow.model.traversal.ITraversalRequest;

public class GeaFlowDynamicVCAggTraversalFunction implements
    IncVertexCentricAggTraversalFunction<Object, Row, Row, MessageBox, ITreePath, ITraversalAgg,
        ITraversalAgg>, RichIteratorFunction {

    private GeaFlowDynamicTraversalRuntimeContext traversalRuntimeContext;

    private final GeaFlowCommonTraversalFunction commonFunction;

    private MutableGraph<Object, Row, Row> mutableGraph;

    public GeaFlowDynamicVCAggTraversalFunction(ExecuteDagGroup executeDagGroup, boolean isTraversalAllWithRequest) {
        this.commonFunction = new GeaFlowCommonTraversalFunction(executeDagGroup, isTraversalAllWithRequest);
    }

    @Override
    public void open(
        IncVertexCentricTraversalFuncContext<Object, Row, Row, MessageBox, ITreePath> vertexCentricFuncContext) {
        traversalRuntimeContext = new GeaFlowDynamicTraversalRuntimeContext(
            vertexCentricFuncContext);
        this.mutableGraph = vertexCentricFuncContext.getMutableGraph();
        this.commonFunction.open(traversalRuntimeContext);
    }

    @Override
    public void evolve(Object vertexId, TemporaryGraph<Object, Row, Row> temporaryGraph) {
        IVertex<Object, Row> vertex = temporaryGraph.getVertex();
        if (vertex != null) {
            mutableGraph.addVertex(GRAPH_VERSION, vertex);
        }
        List<IEdge<Object, Row>> edges = temporaryGraph.getEdges();
        if (edges != null) {
            for (IEdge<Object, Row> edge : edges) {
                mutableGraph.addEdge(GRAPH_VERSION, edge);
            }
        }
    }

    @Override
    public void initIteration(long windowId) {

    }

    @Override
    public void init(ITraversalRequest<Object> traversalRequest) {
        commonFunction.init(traversalRequest);
    }

    @Override
    public void finish() {

    }

    @Override
    public void close() {

    }

    @Override
    public void compute(Object vertexId, Iterator<MessageBox> messageIterator) {
        commonFunction.compute(vertexId, messageIterator);
    }

    @Override
    public void finishIteration(long windowId) {
        commonFunction.finish(windowId);
    }

    @Override
    public void finish(Object vertexId, MutableGraph<Object, Row, Row> mutableGraph) {

    }

    @Override
    public void initContext(VertexCentricAggContext<ITraversalAgg, ITraversalAgg> aggContext) {
        this.traversalRuntimeContext.setAggContext(Objects.requireNonNull(aggContext));
    }
}
