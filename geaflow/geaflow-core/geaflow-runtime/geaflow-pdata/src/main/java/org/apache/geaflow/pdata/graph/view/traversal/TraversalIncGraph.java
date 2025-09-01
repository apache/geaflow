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

package org.apache.geaflow.pdata.graph.view.traversal;

import com.google.common.collect.Lists;
import java.util.ArrayList;
import java.util.List;
import org.apache.geaflow.api.graph.base.algo.AbstractIncVertexCentricTraversalAlgo;
import org.apache.geaflow.api.graph.base.algo.GraphAggregationAlgo;
import org.apache.geaflow.api.graph.base.algo.GraphExecAlgo;
import org.apache.geaflow.api.graph.traversal.IncVertexCentricAggTraversal;
import org.apache.geaflow.api.graph.traversal.IncVertexCentricTraversal;
import org.apache.geaflow.api.graph.traversal.PGraphTraversal;
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
import org.apache.geaflow.pdata.graph.view.AbstractGraphView;
import org.apache.geaflow.pdata.stream.TransformType;
import org.apache.geaflow.pipeline.context.IPipelineContext;
import org.apache.geaflow.view.IViewDesc;
import org.apache.geaflow.view.graph.GraphViewDesc;

public class TraversalIncGraph<K, VV, EV, M, R> extends AbstractGraphView<K, VV, EV, M,
    ITraversalResponse<R>> implements PGraphTraversal<K, R> {

    protected PWindowStream<? extends ITraversalRequest<K>> requestStream;
    protected AbstractIncVertexCentricTraversalAlgo<K, VV, EV, M, R, ?> incVertexCentricTraversal;

    public TraversalIncGraph(IPipelineContext pipelineContext,
                             IViewDesc graphViewDesc,
                             PWindowStream<IVertex<K, VV>> vertexWindowStream,
                             PWindowStream<IEdge<K, EV>> edgeWindowStream) {
        super(pipelineContext, graphViewDesc, vertexWindowStream, edgeWindowStream);
        this.vertexStream = vertexWindowStream;
        this.edgeStream = edgeWindowStream;
        this.graphViewDesc = (GraphViewDesc) graphViewDesc;
        super.parallelism = Math.max(vertexStream.getParallelism(), edgeStream.getParallelism());
    }

    public TraversalIncGraph<K, VV, EV, M, R> traversalOnVertexCentric(
        AbstractIncVertexCentricTraversalAlgo<K, VV, EV, M, R, ?> incVertexCentricTraversal) {
        processOnVertexCentric(incVertexCentricTraversal);
        this.incVertexCentricTraversal = incVertexCentricTraversal;
        return this;
    }

    @Override
    public PWindowStream<ITraversalResponse<R>> start() {
        IGraphVertexCentricOp<K, VV, EV, M> traversalOp;
        if (incVertexCentricTraversal instanceof GraphAggregationAlgo) {
            traversalOp = GraphVertexCentricOpFactory.buildDynamicGraphVertexCentricTraversalAllOp(graphViewDesc,
                (IncVertexCentricAggTraversal<K, VV, EV, M, R, ?, ?, ?, ?, ?>) incVertexCentricTraversal);
        } else {
            traversalOp = GraphVertexCentricOpFactory.buildDynamicGraphVertexCentricTraversalAllOp(graphViewDesc,
                (IncVertexCentricTraversal<K, VV, EV, M, R>) incVertexCentricTraversal);
        }
        super.operator = (Operator) traversalOp;
        this.opArgs = ((AbstractOperator) operator).getOpArgs();
        this.opArgs.setOpId(getId());
        this.opArgs.setOpName(incVertexCentricTraversal.getName());
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
            VertexBeginTraversalRequest<K> vertexBeginTraversalRequest =
                new VertexBeginTraversalRequest<>(
                    vId);
            vertexBeginTraversalRequests.add(vertexBeginTraversalRequest);
        }

        IGraphVertexCentricOp<K, VV, EV, M> traversalOp;
        if (incVertexCentricTraversal instanceof GraphAggregationAlgo) {
            traversalOp = GraphVertexCentricOpFactory.buildDynamicGraphVertexCentricTraversalOp(
                graphViewDesc,
                (IncVertexCentricAggTraversal<K, VV, EV, M, R, ?, ?, ?, ?, ?>) incVertexCentricTraversal,
                vertexBeginTraversalRequests);
        } else {
            traversalOp = GraphVertexCentricOpFactory.buildDynamicGraphVertexCentricTraversalOp(
                graphViewDesc,
                (IncVertexCentricTraversal<K, VV, EV, M, R>) incVertexCentricTraversal,
                vertexBeginTraversalRequests);
        }
        super.operator = (Operator) traversalOp;
        this.opArgs = ((AbstractOperator) operator).getOpArgs();
        this.opArgs.setOpId(getId());
        this.opArgs.setOpName(incVertexCentricTraversal.getName());
        this.opArgs.setParallelism(this.parallelism);
        return this;
    }

    @Override
    public PWindowStream<ITraversalResponse<R>> start(
        PWindowStream<? extends ITraversalRequest<K>> requests) {
        this.requestStream = requests instanceof PWindowBroadcastStream
            ? requests : requests.keyBy(new DefaultTraversalRequestPartition());
        IGraphVertexCentricOp<K, VV, EV, M> traversalOp;
        if (incVertexCentricTraversal instanceof GraphAggregationAlgo) {
            traversalOp = GraphVertexCentricOpFactory.buildDynamicGraphVertexCentricTraversalOp(
                graphViewDesc,
                (IncVertexCentricAggTraversal<K, VV, EV, M, R, ?, ?, ?, ?, ?>) incVertexCentricTraversal);
        } else {
            traversalOp = GraphVertexCentricOpFactory.buildDynamicGraphVertexCentricTraversalOp(
                graphViewDesc,
                (IncVertexCentricTraversal<K, VV, EV, M, R>) incVertexCentricTraversal);
        }
        super.operator = (Operator) traversalOp;
        this.opArgs = ((AbstractOperator) operator).getOpArgs();
        this.opArgs.setOpId(getId());
        this.opArgs.setOpName(incVertexCentricTraversal.getName());
        this.opArgs.setParallelism(this.parallelism);
        return this;
    }

    @Override
    public GraphExecAlgo getGraphTraversalType() {
        return graphExecAlgo;
    }

    @Override
    public TraversalIncGraph<K, VV, EV, M, R> withParallelism(int parallelism) {
        setParallelism(parallelism);
        return this;
    }

    public PWindowStream<? extends ITraversalRequest<K>> getRequestStream() {
        return requestStream;
    }

    @Override
    public TransformType getTransformType() {
        return TransformType.ContinueGraphTraversal;
    }
}
