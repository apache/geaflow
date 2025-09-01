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

package org.apache.geaflow.pdata.graph.view.compute;

import com.google.common.base.Preconditions;
import org.apache.geaflow.api.graph.base.algo.GraphExecAlgo;
import org.apache.geaflow.api.graph.compute.IncVertexCentricAggCompute;
import org.apache.geaflow.api.graph.compute.IncVertexCentricCompute;
import org.apache.geaflow.api.graph.compute.PGraphCompute;
import org.apache.geaflow.api.pdata.stream.window.PWindowStream;
import org.apache.geaflow.model.graph.edge.IEdge;
import org.apache.geaflow.model.graph.vertex.IVertex;
import org.apache.geaflow.operator.Operator;
import org.apache.geaflow.operator.base.AbstractOperator;
import org.apache.geaflow.operator.impl.graph.algo.vc.IGraphVertexCentricOp;
import org.apache.geaflow.operator.impl.graph.compute.dynamic.DynamicGraphVertexCentricComputeOp;
import org.apache.geaflow.operator.impl.graph.compute.dynamic.DynamicGraphVertexCentricComputeWithAggOp;
import org.apache.geaflow.pdata.graph.view.AbstractGraphView;
import org.apache.geaflow.pdata.stream.TransformType;
import org.apache.geaflow.pipeline.context.IPipelineContext;
import org.apache.geaflow.view.IViewDesc;

public class ComputeIncGraph<K, VV, EV, M> extends AbstractGraphView<K, VV, EV, M,
    IVertex<K, VV>> implements PGraphCompute<K, VV, EV> {

    public ComputeIncGraph(IPipelineContext pipelineContext,
                           IViewDesc graphViewDesc,
                           PWindowStream<IVertex<K, VV>> vertexWindowStream,
                           PWindowStream<IEdge<K, EV>> edgeWindowStream) {
        super(pipelineContext, graphViewDesc, vertexWindowStream, edgeWindowStream);
    }

    public PWindowStream<IVertex<K, VV>> computeOnIncVertexCentric(
        IncVertexCentricCompute<K, VV, EV, M> incVertexCentricCompute) {
        processOnVertexCentric(incVertexCentricCompute);
        IGraphVertexCentricOp<K, VV, EV, M> graphVertexCentricComputeOp =
            new DynamicGraphVertexCentricComputeOp(graphViewDesc, incVertexCentricCompute);
        super.operator = (Operator) graphVertexCentricComputeOp;
        this.opArgs = ((AbstractOperator) operator).getOpArgs();
        this.opArgs.setOpId(getId());
        this.opArgs.setOpName(incVertexCentricCompute.getName());

        return this;
    }

    public <I, PA, PR, GA, GR> PWindowStream<IVertex<K, VV>> computeOnIncVertexCentric(
        IncVertexCentricAggCompute<K, VV, EV, M, I, PA, PR, GA, GR> incVertexCentricCompute) {
        processOnVertexCentric(incVertexCentricCompute);
        IGraphVertexCentricOp<K, VV, EV, M> graphVertexCentricComputeOp =
            new DynamicGraphVertexCentricComputeWithAggOp(graphViewDesc, incVertexCentricCompute);
        super.operator = (Operator) graphVertexCentricComputeOp;
        this.opArgs = ((AbstractOperator) operator).getOpArgs();
        this.opArgs.setOpId(getId());
        this.opArgs.setOpName(incVertexCentricCompute.getName());

        return this;
    }

    @Override
    public PGraphCompute<K, VV, EV> compute() {
        return this;
    }

    @Override
    public PGraphCompute<K, VV, EV> compute(int parallelism) {
        Preconditions.checkArgument(parallelism <= graphViewDesc.getShardNum(),
            "op parallelism must be <= shard num");
        super.parallelism = parallelism;
        return this;
    }

    @Override
    public PWindowStream<IVertex<K, VV>> getVertices() {
        return this;
    }


    @Override
    public GraphExecAlgo getGraphComputeType() {
        return graphExecAlgo;
    }

    @Override
    public TransformType getTransformType() {
        return TransformType.ContinueGraphCompute;
    }


}
