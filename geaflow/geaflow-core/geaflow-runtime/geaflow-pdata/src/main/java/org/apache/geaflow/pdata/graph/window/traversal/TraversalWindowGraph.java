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

package org.apache.geaflow.pdata.graph.window.traversal;

import com.google.common.collect.Lists;
import java.util.ArrayList;
import java.util.List;
import org.apache.geaflow.api.graph.base.algo.AbstractVertexCentricTraversalAlgo;
import org.apache.geaflow.api.graph.base.algo.GraphAggregationAlgo;
import org.apache.geaflow.api.graph.base.algo.GraphExecAlgo;
import org.apache.geaflow.api.graph.traversal.PGraphTraversal;
import org.apache.geaflow.api.graph.traversal.VertexCentricAggTraversal;
import org.apache.geaflow.api.graph.traversal.VertexCentricTraversal;
import org.apache.geaflow.api.partition.graph.request.DefaultTraversalRequestPartition;
import org.apache.geaflow.api.pdata.stream.window.PWindowBroadcastStream;
import org.apache.geaflow.api.pdata.stream.window.PWindowStream;
import org.apache.geaflow.model.graph.edge.IEdge;
import org.apache.geaflow.model.graph.vertex.IVertex;
import org.apache.geaflow.model.traversal.ITraversalRequest;
import org.apache.geaflow.model.traversal.ITraversalResponse;
import org.apache.geaflow.model.traversal.impl.VertexBeginTraversalRequest;
import org.apache.geaflow.operator.Operator;
import org.apache.geaflow.operator.base.AbstractOperator;
import org.apache.geaflow.operator.impl.graph.algo.vc.GraphVertexCentricOpFactory;
import org.apache.geaflow.operator.impl.graph.algo.vc.IGraphVertexCentricOp;
import org.apache.geaflow.pdata.graph.window.AbstractGraphWindow;
import org.apache.geaflow.pdata.stream.Stream;
import org.apache.geaflow.pdata.stream.TransformType;
import org.apache.geaflow.pipeline.context.IPipelineContext;
import org.apache.geaflow.view.graph.GraphViewDesc;

public class TraversalWindowGraph<K, VV, EV, M, R> extends
    AbstractGraphWindow<K, VV, EV, M, ITraversalResponse<R>> implements PGraphTraversal<K, R> {

    protected PWindowStream<? extends ITraversalRequest<K>> requestStream;
    protected AbstractVertexCentricTraversalAlgo<K, VV, EV, M, R, ?> vertexCentricTraversal;
    private final GraphViewDesc graphViewDesc;

    public TraversalWindowGraph(GraphViewDesc graphViewDesc,
                                IPipelineContext pipelineContext,
                                PWindowStream<IVertex<K, VV>> vertexWindowStream,
                                PWindowStream<IEdge<K, EV>> edgeWindowStream) {
        super(pipelineContext, vertexWindowStream, edgeWindowStream);
        super.input = (Stream) vertexWindowStream;
        this.edgeStream = edgeWindowStream;
        this.graphViewDesc = graphViewDesc;
    }

    public void traversalOnVertexCentric(VertexCentricTraversal<K, VV, EV, M, R> vertexCentricTraversal) {
        this.vertexCentricTraversal = vertexCentricTraversal;
        processOnVertexCentric(vertexCentricTraversal);
        this.graphExecAlgo = GraphExecAlgo.VertexCentric;
        this.maxIterations = vertexCentricTraversal.getMaxIterationCount();
    }

    public <I, PA, PR, GA, GR> void traversalOnVertexCentric(
        VertexCentricAggTraversal<K, VV, EV, M, R, I, PA, PR, GA, GR> vertexCentricTraversal) {
        this.vertexCentricTraversal = vertexCentricTraversal;
        processOnVertexCentric(vertexCentricTraversal);
        this.graphExecAlgo = GraphExecAlgo.VertexCentric;
        this.maxIterations = vertexCentricTraversal.getMaxIterationCount();
    }

    @Override
    public PWindowStream<ITraversalResponse<R>> start() {
        IGraphVertexCentricOp<K, VV, EV, M> traversalOp;
        if (vertexCentricTraversal instanceof GraphAggregationAlgo) {
            traversalOp = GraphVertexCentricOpFactory.buildStaticGraphVertexCentricAggTraversalAllOp(graphViewDesc,
                (VertexCentricAggTraversal) vertexCentricTraversal);
        } else {
            traversalOp = GraphVertexCentricOpFactory.buildStaticGraphVertexCentricTraversalAllOp(graphViewDesc,
                (VertexCentricTraversal<K, VV, EV, M, R>) vertexCentricTraversal);
        }
        super.operator = (Operator) traversalOp;
        this.opArgs = ((AbstractOperator) operator).getOpArgs();
        this.opArgs.setOpId(getId());
        this.opArgs.setOpName(vertexCentricTraversal.getName());
        this.opArgs.setParallelism(this.parallelism);
        return this;
    }

    @Override
    public PWindowStream<ITraversalResponse<R>> start(K vId) {
        return start(Lists.newArrayList(vId));
    }

    @Override
    public PWindowStream<ITraversalResponse<R>> start(List<K> vIds) {
        List<VertexBeginTraversalRequest<K>> vertexBeginTraversalRequests = new ArrayList<>();
        for (K vId : vIds) {
            VertexBeginTraversalRequest<K> vertexBeginTraversalRequest = new VertexBeginTraversalRequest(
                vId);
            vertexBeginTraversalRequests.add(vertexBeginTraversalRequest);
        }
        IGraphVertexCentricOp<K, VV, EV, M> traversalOp;
        if (vertexCentricTraversal instanceof GraphAggregationAlgo) {
            traversalOp = GraphVertexCentricOpFactory.buildStaticGraphVertexCentricAggTraversalOp(graphViewDesc,
                (VertexCentricAggTraversal) vertexCentricTraversal, vertexBeginTraversalRequests);
        } else {
            traversalOp = GraphVertexCentricOpFactory.buildStaticGraphVertexCentricTraversalOp(graphViewDesc,
                (VertexCentricTraversal<K, VV, EV, M, R>) vertexCentricTraversal, vertexBeginTraversalRequests);
        }
        super.operator = (Operator) traversalOp;
        this.opArgs = ((AbstractOperator) operator).getOpArgs();
        this.opArgs.setOpId(getId());
        this.opArgs.setOpName(vertexCentricTraversal.getName());
        this.opArgs.setParallelism(this.parallelism);
        return this;
    }

    @Override
    public PWindowStream<ITraversalResponse<R>> start(
        PWindowStream<? extends ITraversalRequest<K>> requests) {
        this.requestStream = requests instanceof PWindowBroadcastStream
            ? requests : requests.keyBy(new DefaultTraversalRequestPartition());
        IGraphVertexCentricOp<K, VV, EV, M> traversalOp;
        if (vertexCentricTraversal instanceof GraphAggregationAlgo) {
            traversalOp = GraphVertexCentricOpFactory.buildStaticGraphVertexCentricAggTraversalOp(graphViewDesc,
                (VertexCentricAggTraversal) vertexCentricTraversal);
        } else {
            traversalOp = GraphVertexCentricOpFactory.buildStaticGraphVertexCentricTraversalOp(graphViewDesc,
                (VertexCentricTraversal<K, VV, EV, M, R>) vertexCentricTraversal);
        }
        super.operator = (Operator) traversalOp;
        this.opArgs = ((AbstractOperator) operator).getOpArgs();
        this.opArgs.setOpId(getId());
        this.opArgs.setOpName(vertexCentricTraversal.getName());
        this.opArgs.setParallelism(this.parallelism);
        return this;
    }

    @Override
    public TraversalWindowGraph<K, VV, EV, M, R> withParallelism(int parallelism) {
        setParallelism(parallelism);
        return this;
    }

    @Override
    public GraphExecAlgo getGraphTraversalType() {
        return graphExecAlgo;
    }

    @Override
    public TransformType getTransformType() {
        return TransformType.WindowGraphTraversal;
    }

    public PWindowStream<? extends ITraversalRequest<K>> getRequestStream() {
        return requestStream;
    }
}
