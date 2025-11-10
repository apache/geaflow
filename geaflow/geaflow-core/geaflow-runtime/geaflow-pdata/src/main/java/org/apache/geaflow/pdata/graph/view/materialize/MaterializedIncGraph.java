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

package org.apache.geaflow.pdata.graph.view.materialize;

import java.util.Map;
import org.apache.geaflow.api.graph.materialize.PGraphMaterialize;
import org.apache.geaflow.api.pdata.stream.window.PWindowStream;
import org.apache.geaflow.model.common.Null;
import org.apache.geaflow.model.graph.edge.IEdge;
import org.apache.geaflow.model.graph.vertex.IVertex;
import org.apache.geaflow.operator.base.AbstractOperator;
import org.apache.geaflow.operator.impl.graph.materialize.GraphViewMaterializeOp;
import org.apache.geaflow.operator.impl.graph.materialize.PaimonGlobalSink;
import org.apache.geaflow.operator.impl.window.SinkOperator;
import org.apache.geaflow.partitioner.IPartitioner;
import org.apache.geaflow.partitioner.impl.KeyPartitioner;
import org.apache.geaflow.pdata.graph.view.AbstractGraphView;
import org.apache.geaflow.pdata.graph.window.WindowStreamGraph;
import org.apache.geaflow.pdata.stream.Stream;
import org.apache.geaflow.pdata.stream.TransformType;
import org.apache.geaflow.pdata.stream.window.WindowStreamSink;
import org.apache.geaflow.pipeline.context.IPipelineContext;
import org.apache.geaflow.store.paimon.commit.PaimonMessage;
import org.apache.geaflow.view.IViewDesc;
import org.apache.geaflow.view.IViewDesc.BackendType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MaterializedIncGraph<K, VV, EV> extends AbstractGraphView<K, VV, EV, Null,
    IVertex<K, VV>> implements PGraphMaterialize<K, VV, EV> {

    private static final Logger LOGGER = LoggerFactory.getLogger(MaterializedIncGraph.class);

    public MaterializedIncGraph(IPipelineContext pipelineContext,
                                IViewDesc graphViewDesc,
                                PWindowStream<IVertex<K, VV>> vertexWindowStream,
                                PWindowStream<IEdge<K, EV>> edgeWindowStream) {
        super(pipelineContext, graphViewDesc, vertexWindowStream, edgeWindowStream);
    }

    @Override
    public void materialize() {
        LOGGER.info("call materialize");
        GraphViewMaterializeOp materializeOp = new GraphViewMaterializeOp<>(graphViewDesc);
        super.operator = materializeOp;
        this.opArgs = ((AbstractOperator) operator).getOpArgs();
        this.opArgs.setOpId(getId());
        this.withParallelism(graphViewDesc.getShardNum());
        this.opArgs.setOpName(MaterializedIncGraph.class.getSimpleName());
        assert this.vertexStream.getParallelism() <= graphViewDesc.getShardNum() : "Materialize "
            + "vertexStream parallelism must <= number of graph shard num";
        this.input = (Stream) this.vertexStream
            .keyBy(new WindowStreamGraph.DefaultVertexPartition<>());
        assert this.edgeStream.getParallelism() <= graphViewDesc.getShardNum() : "Materialize "
            + "edgeStream parallelism must <= number of graph shard num";
        this.edgeStream = this.edgeStream
            .keyBy(new WindowStreamGraph.DefaultEdgePartition<>());

        if (graphViewDesc.getBackend() == BackendType.Paimon) {
            SinkOperator<PaimonMessage> operator =
                new SinkOperator<>(new PaimonGlobalSink());
            WindowStreamSink globalSink = new WindowStreamSink<>(this, operator);
            globalSink.withParallelism(1);
            super.context.addPAction(globalSink);
        } else {
            super.context.addPAction(this);
        }

        assert this.getParallelism() == graphViewDesc.getShardNum() : "Materialize parallelism "
            + "must be equal to the graph shard num.";
    }

    @Override
    public TransformType getTransformType() {
        return TransformType.ContinueGraphMaterialize;
    }

    @Override
    public IPartitioner getPartition() {
        return new KeyPartitioner(this.getId());
    }

    @Override
    public MaterializedIncGraph<K, VV, EV> withConfig(Map config) {
        this.setConfig(config);
        return this;
    }

    @Override
    public MaterializedIncGraph<K, VV, EV> withConfig(String key, String value) {
        this.setConfig(key, value);
        return this;
    }

    @Override
    public MaterializedIncGraph<K, VV, EV> withName(String name) {
        this.setName(name);
        return this;
    }

    @Override
    public MaterializedIncGraph<K, VV, EV> withParallelism(int parallelism) {
        this.setParallelism(parallelism);
        return this;
    }
}
