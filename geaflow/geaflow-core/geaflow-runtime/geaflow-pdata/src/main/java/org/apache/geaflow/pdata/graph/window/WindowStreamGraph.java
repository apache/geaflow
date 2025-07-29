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

package org.apache.geaflow.pdata.graph.window;

import com.google.common.base.Preconditions;
import java.io.Serializable;
import org.apache.geaflow.api.function.base.KeySelector;
import org.apache.geaflow.api.function.internal.CollectionSource;
import org.apache.geaflow.api.graph.PGraphWindow;
import org.apache.geaflow.api.graph.compute.PGraphCompute;
import org.apache.geaflow.api.graph.compute.VertexCentricAggCompute;
import org.apache.geaflow.api.graph.compute.VertexCentricCompute;
import org.apache.geaflow.api.graph.traversal.PGraphTraversal;
import org.apache.geaflow.api.graph.traversal.VertexCentricAggTraversal;
import org.apache.geaflow.api.graph.traversal.VertexCentricTraversal;
import org.apache.geaflow.api.pdata.stream.window.PWindowStream;
import org.apache.geaflow.api.window.impl.AllWindow;
import org.apache.geaflow.model.graph.edge.IEdge;
import org.apache.geaflow.model.graph.vertex.IVertex;
import org.apache.geaflow.pdata.graph.window.compute.ComputeWindowGraph;
import org.apache.geaflow.pdata.graph.window.traversal.TraversalWindowGraph;
import org.apache.geaflow.pdata.stream.window.WindowStreamSource;
import org.apache.geaflow.pipeline.context.IPipelineContext;
import org.apache.geaflow.view.graph.GraphViewDesc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WindowStreamGraph<K, VV, EV> implements PGraphWindow<K, VV, EV>, Serializable {

    private static final Logger LOGGER = LoggerFactory.getLogger(WindowStreamGraph.class);

    private final GraphViewDesc graphViewDesc;
    private final IPipelineContext pipelineContext;
    private final PWindowStream<IVertex<K, VV>> vertexWindowSteam;
    private final PWindowStream<IEdge<K, EV>> edgeWindowStream;

    /**
     * Create a static window graph.
     */
    public WindowStreamGraph(GraphViewDesc graphViewDesc, IPipelineContext pipelineContext,
                             PWindowStream<IVertex<K, VV>> vertexWindowSteam,
                             PWindowStream<IEdge<K, EV>> edgeWindowStream) {
        this.graphViewDesc = graphViewDesc.asStatic();
        this.pipelineContext = pipelineContext;
        this.vertexWindowSteam = vertexWindowSteam;
        this.edgeWindowStream = edgeWindowStream;
    }

    /**
     * Create a snapshot window graph.
     */
    public WindowStreamGraph(GraphViewDesc graphViewDesc, IPipelineContext pipelineContext) {
        this.graphViewDesc = graphViewDesc;
        this.pipelineContext = pipelineContext;
        this.vertexWindowSteam = new WindowStreamSource(pipelineContext, new CollectionSource(),
            AllWindow.getInstance());
        this.edgeWindowStream = new WindowStreamSource(pipelineContext, new CollectionSource(),
            AllWindow.getInstance());
    }


    @Override
    public <M> PGraphCompute<K, VV, EV> compute(VertexCentricCompute<K, VV, EV, M> vertexCentricCompute) {
        Preconditions.checkArgument(vertexCentricCompute.getMaxIterationCount() > 0);
        ComputeWindowGraph<K, VV, EV, M> graphCompute = new ComputeWindowGraph<>(pipelineContext,
            vertexWindowSteam, edgeWindowStream);
        graphCompute.computeOnVertexCentric(graphViewDesc, vertexCentricCompute);
        return graphCompute;
    }

    @Override
    public <M, I, PA, PR, GA, R> PGraphCompute<K, VV, EV> compute(
        VertexCentricAggCompute<K, VV, EV, M, I, PA, PR, GA, R> vertexCentricAggCompute) {
        Preconditions.checkArgument(vertexCentricAggCompute.getMaxIterationCount() > 0);
        ComputeWindowGraph<K, VV, EV, M> graphCompute = new ComputeWindowGraph<>(pipelineContext,
            vertexWindowSteam, edgeWindowStream);
        graphCompute.computeOnVertexCentric(graphViewDesc, vertexCentricAggCompute);
        return graphCompute;
    }


    public static class DefaultVertexPartition<K, VV> implements KeySelector<IVertex<K, VV>, K> {
        @Override
        public K getKey(IVertex<K, VV> value) {
            return value.getId();
        }
    }

    public static class DefaultEdgePartition<K, EV> implements KeySelector<IEdge<K, EV>, K> {
        @Override
        public K getKey(IEdge<K, EV> value) {
            return value.getSrcId();
        }
    }

    @Override
    public <M, R> PGraphTraversal<K, R> traversal(
        VertexCentricTraversal<K, VV, EV, M, R> vertexCentricTraversal) {
        TraversalWindowGraph<K, VV, EV, M, R> traversalWindowGraph =
            new TraversalWindowGraph<>(graphViewDesc, pipelineContext,
                vertexWindowSteam,
                edgeWindowStream);
        traversalWindowGraph.traversalOnVertexCentric(vertexCentricTraversal);
        return traversalWindowGraph;
    }

    @Override
    public <M, R, I, PA, PR, GA, GR> PGraphTraversal<K, R> traversal(
        VertexCentricAggTraversal<K, VV, EV, M, R, I, PA, PR, GA, GR> vertexCentricAggTraversal) {
        TraversalWindowGraph<K, VV, EV, M, R> traversalWindowGraph =
            new TraversalWindowGraph<>(graphViewDesc, pipelineContext,
                vertexWindowSteam,
                edgeWindowStream);
        traversalWindowGraph.traversalOnVertexCentric(vertexCentricAggTraversal);
        return traversalWindowGraph;
    }

    @Override
    public PWindowStream<IEdge<K, EV>> getEdges() {
        return this.edgeWindowStream;
    }

    @Override
    public PWindowStream<IVertex<K, VV>> getVertices() {
        return this.vertexWindowSteam;
    }
}
