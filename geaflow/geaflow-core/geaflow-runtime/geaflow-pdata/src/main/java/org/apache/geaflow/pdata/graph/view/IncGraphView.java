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

package org.apache.geaflow.pdata.graph.view;

import com.google.common.annotations.VisibleForTesting;
import org.apache.geaflow.api.graph.PGraphWindow;
import org.apache.geaflow.api.graph.compute.IncVertexCentricAggCompute;
import org.apache.geaflow.api.graph.compute.IncVertexCentricCompute;
import org.apache.geaflow.api.graph.compute.PGraphCompute;
import org.apache.geaflow.api.graph.traversal.IncVertexCentricAggTraversal;
import org.apache.geaflow.api.graph.traversal.IncVertexCentricTraversal;
import org.apache.geaflow.api.graph.traversal.PGraphTraversal;
import org.apache.geaflow.api.pdata.stream.window.PWindowStream;
import org.apache.geaflow.model.graph.edge.IEdge;
import org.apache.geaflow.model.graph.vertex.IVertex;
import org.apache.geaflow.pdata.graph.view.compute.ComputeIncGraph;
import org.apache.geaflow.pdata.graph.view.materialize.MaterializedIncGraph;
import org.apache.geaflow.pdata.graph.view.traversal.TraversalIncGraph;
import org.apache.geaflow.pdata.graph.window.WindowStreamGraph;
import org.apache.geaflow.pipeline.context.IPipelineContext;
import org.apache.geaflow.view.IViewDesc;
import org.apache.geaflow.view.graph.GraphViewDesc;
import org.apache.geaflow.view.graph.PGraphView;
import org.apache.geaflow.view.graph.PIncGraphView;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IncGraphView<K, VV, EV> implements PIncGraphView<K, VV, EV> {

    private static final Logger LOGGER = LoggerFactory.getLogger(IncGraphView.class);

    private IPipelineContext pipelineContext;
    private PWindowStream<IVertex<K, VV>> vertexWindowSteam;
    private PWindowStream<IEdge<K, EV>> edgeWindowStream;
    private IViewDesc graphViewDesc;

    @VisibleForTesting
    private MaterializedIncGraph<K, VV, EV> materializedIncGraph;

    public IncGraphView(IPipelineContext pipelineContext, IViewDesc viewDesc) {
        this.pipelineContext = pipelineContext;
        this.graphViewDesc = viewDesc;
    }

    @Override
    public PGraphView<K, VV, EV> init(GraphViewDesc graphViewDesc) {
        this.graphViewDesc = graphViewDesc;
        return this;
    }

    @SuppressWarnings("unchecked")
    @Override
    public PGraphWindow<K, VV, EV> snapshot(long version) {
        return new WindowStreamGraph<>(((GraphViewDesc) graphViewDesc).snapshot(version), pipelineContext);
    }

    @Override
    public PIncGraphView<K, VV, EV> appendGraph(PWindowStream<IVertex<K, VV>> vertexStream,
                                                PWindowStream<IEdge<K, EV>> edgeStream) {
        this.vertexWindowSteam = vertexStream;
        this.edgeWindowStream = edgeStream;
        return this;
    }

    @Override
    public PIncGraphView<K, VV, EV> appendEdge(PWindowStream<IEdge<K, EV>> edgeStream) {
        this.edgeWindowStream = edgeStream;
        return this;
    }

    @Override
    public PIncGraphView<K, VV, EV> appendVertex(PWindowStream<IVertex<K, VV>> vertexStream) {
        this.vertexWindowSteam = vertexStream;
        return this;
    }

    @Override
    public <M> PGraphCompute<K, VV, EV> incrementalCompute(
        IncVertexCentricCompute<K, VV, EV, M> incVertexCentricCompute) {
        ComputeIncGraph<K, VV, EV, M> computeIncGraph = new ComputeIncGraph<>(pipelineContext,
            graphViewDesc, vertexWindowSteam, edgeWindowStream);
        computeIncGraph.computeOnIncVertexCentric(incVertexCentricCompute);
        return computeIncGraph;
    }

    @Override
    public <M, I, PA, PR, GA, GR> PGraphCompute<K, VV, EV> incrementalCompute(
        IncVertexCentricAggCompute<K, VV, EV, M, I, PA, PR, GA, GR> incVertexCentricCompute) {
        ComputeIncGraph<K, VV, EV, M> computeIncGraph =
            new ComputeIncGraph<>(pipelineContext, graphViewDesc,
                vertexWindowSteam,
                edgeWindowStream);
        computeIncGraph.computeOnIncVertexCentric(incVertexCentricCompute);
        return null;
    }

    @Override
    public <M, R> PGraphTraversal<K, R> incrementalTraversal(
        IncVertexCentricTraversal<K, VV, EV, M, R> incVertexCentricTraversal) {

        TraversalIncGraph<K, VV, EV, M, R> traversalIncGraph =
            new TraversalIncGraph<>(pipelineContext, graphViewDesc,
                this.vertexWindowSteam,
                this.edgeWindowStream);
        traversalIncGraph.traversalOnVertexCentric(incVertexCentricTraversal);
        return traversalIncGraph;
    }

    @Override
    public <M, R, I, PA, PR, GA, GR> PGraphTraversal<K, R> incrementalTraversal(
        IncVertexCentricAggTraversal<K, VV, EV, M, R, I, PA, PR, GA, GR> incVertexCentricTraversal) {
        TraversalIncGraph<K, VV, EV, M, R> traversalIncGraph =
            new TraversalIncGraph<>(pipelineContext, graphViewDesc,
                this.vertexWindowSteam,
                this.edgeWindowStream);
        traversalIncGraph.traversalOnVertexCentric(incVertexCentricTraversal);
        return traversalIncGraph;
    }

    @Override
    public void materialize() {
        materializedIncGraph =
            new MaterializedIncGraph(pipelineContext, graphViewDesc, vertexWindowSteam, edgeWindowStream);
        materializedIncGraph.materialize();
    }

    @VisibleForTesting
    public MaterializedIncGraph getMaterializedIncGraph() {
        return materializedIncGraph;
    }
}
