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

package org.apache.geaflow.pdata.graph.window.compute;

import org.apache.geaflow.api.graph.base.algo.GraphExecAlgo;
import org.apache.geaflow.api.graph.compute.PGraphCompute;
import org.apache.geaflow.api.graph.compute.VertexCentricAggCompute;
import org.apache.geaflow.api.graph.compute.VertexCentricCompute;
import org.apache.geaflow.api.pdata.stream.window.PWindowStream;
import org.apache.geaflow.model.graph.edge.IEdge;
import org.apache.geaflow.model.graph.vertex.IVertex;
import org.apache.geaflow.operator.Operator;
import org.apache.geaflow.operator.base.AbstractOperator;
import org.apache.geaflow.operator.impl.graph.algo.vc.GraphVertexCentricOpFactory;
import org.apache.geaflow.operator.impl.graph.algo.vc.IGraphVertexCentricAggOp;
import org.apache.geaflow.operator.impl.graph.algo.vc.IGraphVertexCentricOp;
import org.apache.geaflow.pdata.graph.window.AbstractGraphWindow;
import org.apache.geaflow.pdata.stream.TransformType;
import org.apache.geaflow.pipeline.context.IPipelineContext;
import org.apache.geaflow.view.graph.GraphViewDesc;

public class ComputeWindowGraph<K, VV, EV, M> extends AbstractGraphWindow<K, VV,
    EV, M, IVertex<K, VV>> implements PGraphCompute<K, VV, EV> {

    public ComputeWindowGraph(IPipelineContext pipelineContext,
                              PWindowStream<IVertex<K, VV>> vertexStream,
                              PWindowStream<IEdge<K, EV>> edgeStream) {
        super(pipelineContext, vertexStream, edgeStream);
    }

    public PWindowStream<IVertex<K, VV>> computeOnVertexCentric(GraphViewDesc graphViewDesc,
                                                                VertexCentricCompute<K, VV, EV, M> vertexCentricCompute) {
        processOnVertexCentric(vertexCentricCompute);

        IGraphVertexCentricOp<K, VV, EV, M> graphVertexCentricComputeOp =
            GraphVertexCentricOpFactory.buildStaticGraphVertexCentricComputeOp(graphViewDesc,
                vertexCentricCompute);

        super.operator = (Operator) graphVertexCentricComputeOp;
        this.opArgs = ((AbstractOperator) operator).getOpArgs();
        this.opArgs.setOpId(getId());
        this.opArgs.setOpName(vertexCentricCompute.getName());
        return this;
    }

    public <I, PA, PR, GA, R> PWindowStream<IVertex<K, VV>> computeOnVertexCentric(
        GraphViewDesc graphViewDesc,
        VertexCentricAggCompute<K, VV, EV, M, I, PA, PR, GA, R> vertexCentricAggCompute) {
        processOnVertexCentric(vertexCentricAggCompute);

        IGraphVertexCentricAggOp<K, VV, EV, M, I, PA, PR, R> graphVertexCentricComputeOp =
            GraphVertexCentricOpFactory.buildStaticGraphVertexCentricAggComputeOp(graphViewDesc,
                vertexCentricAggCompute);
        super.operator = (Operator) graphVertexCentricComputeOp;
        this.opArgs = ((AbstractOperator) operator).getOpArgs();
        this.opArgs.setOpId(getId());
        this.opArgs.setOpName(vertexCentricAggCompute.getName());
        return this;
    }


    @Override
    public TransformType getTransformType() {
        return TransformType.WindowGraphCompute;
    }

    @Override
    public PWindowStream<IVertex<K, VV>> getVertices() {
        return this;
    }

    @Override
    public PGraphCompute<K, VV, EV> compute() {
        return this;
    }

    @Override
    public PGraphCompute<K, VV, EV> compute(int parallelism) {
        super.parallelism = parallelism;
        return this;
    }

    @Override
    public GraphExecAlgo getGraphComputeType() {
        return GraphExecAlgo.VertexCentric;
    }

}
