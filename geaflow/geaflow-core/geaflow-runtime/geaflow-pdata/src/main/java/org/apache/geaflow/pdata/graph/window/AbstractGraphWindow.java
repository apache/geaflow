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

import org.apache.geaflow.api.graph.base.algo.GraphExecAlgo;
import org.apache.geaflow.api.graph.base.algo.VertexCentricAlgo;
import org.apache.geaflow.api.partition.graph.edge.CustomEdgeVCPartition;
import org.apache.geaflow.api.partition.graph.edge.CustomVertexVCPartition;
import org.apache.geaflow.api.partition.graph.edge.IGraphVCPartition;
import org.apache.geaflow.api.pdata.stream.window.PWindowStream;
import org.apache.geaflow.common.encoder.EncoderResolver;
import org.apache.geaflow.common.encoder.IEncoder;
import org.apache.geaflow.model.graph.edge.IEdge;
import org.apache.geaflow.model.graph.message.IGraphMessage;
import org.apache.geaflow.model.graph.message.encoder.GraphMessageEncoders;
import org.apache.geaflow.model.graph.vertex.IVertex;
import org.apache.geaflow.pdata.graph.window.WindowStreamGraph.DefaultEdgePartition;
import org.apache.geaflow.pdata.graph.window.WindowStreamGraph.DefaultVertexPartition;
import org.apache.geaflow.pdata.stream.Stream;
import org.apache.geaflow.pdata.stream.window.WindowDataStream;
import org.apache.geaflow.pipeline.context.IPipelineContext;

public abstract class AbstractGraphWindow<K, VV, EV, M, R> extends WindowDataStream<R> {

    protected long maxIterations;
    protected PWindowStream<IVertex<K, VV>> vertexStream;
    protected PWindowStream<IEdge<K, EV>> edgeStream;
    protected GraphExecAlgo graphExecAlgo;
    protected IEncoder<? extends IGraphMessage<K, M>> msgEncoder;

    public AbstractGraphWindow(IPipelineContext pipelineContext,
                               PWindowStream<IVertex<K, VV>> vertexWindowStream,
                               PWindowStream<IEdge<K, EV>> edgeWindowStream) {
        super(pipelineContext);
        this.vertexStream = vertexWindowStream;
        this.edgeStream = edgeWindowStream;
        super.parallelism = Math.max(vertexStream.getParallelism(), edgeStream.getParallelism());
    }

    protected void processOnVertexCentric(VertexCentricAlgo<K, VV, EV, M> vertexCentricAlgo) {
        this.graphExecAlgo = GraphExecAlgo.VertexCentric;
        this.maxIterations = vertexCentricAlgo.getMaxIterationCount();
        IGraphVCPartition<K> graphPartition = vertexCentricAlgo.getGraphPartition();
        if (graphPartition == null) {
            this.input = (Stream) this.vertexStream.keyBy(new DefaultVertexPartition<>());
            this.edgeStream = this.edgeStream.keyBy(new DefaultEdgePartition<>());
        } else {
            this.input = (Stream) this.vertexStream.keyBy(new CustomVertexVCPartition<>(graphPartition));
            this.edgeStream = this.edgeStream.keyBy(new CustomEdgeVCPartition<>(graphPartition));
        }
        IEncoder<K> keyEncoder = vertexCentricAlgo.getKeyEncoder();
        if (keyEncoder == null) {
            keyEncoder = (IEncoder<K>) EncoderResolver.resolveFunction(VertexCentricAlgo.class, vertexCentricAlgo, 0);
        }
        IEncoder<M> msgEncoder = vertexCentricAlgo.getMessageEncoder();
        if (msgEncoder == null) {
            msgEncoder = (IEncoder<M>) EncoderResolver.resolveFunction(VertexCentricAlgo.class, vertexCentricAlgo, 3);
        }
        this.msgEncoder = GraphMessageEncoders.build(keyEncoder, msgEncoder);
    }

    public long getMaxIterations() {
        return maxIterations;
    }

    public PWindowStream<IEdge<K, EV>> getEdges() {
        return this.edgeStream;
    }

    public IEncoder<? extends IGraphMessage<K, M>> getMsgEncoder() {
        return this.msgEncoder;
    }

}
