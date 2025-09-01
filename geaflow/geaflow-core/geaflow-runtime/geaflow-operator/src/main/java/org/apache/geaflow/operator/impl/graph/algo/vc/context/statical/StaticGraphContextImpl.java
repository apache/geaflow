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

package org.apache.geaflow.operator.impl.graph.algo.vc.context.statical;

import org.apache.geaflow.api.context.RuntimeContext;
import org.apache.geaflow.api.graph.function.vc.base.VertexCentricFunction;
import org.apache.geaflow.common.exception.GeaflowRuntimeException;
import org.apache.geaflow.common.iterator.CloseableIterator;
import org.apache.geaflow.model.graph.edge.IEdge;
import org.apache.geaflow.operator.Operator;
import org.apache.geaflow.operator.impl.graph.algo.vc.msgbox.IGraphMsgBox;
import org.apache.geaflow.state.GraphState;

public class StaticGraphContextImpl<K, VV, EV, M>
    implements VertexCentricFunction.VertexCentricFuncContext<K, VV, EV, M> {

    private final Operator.OpContext opContext;
    private final RuntimeContext runtimeContext;
    private final GraphState<K, VV, EV> graphState;
    private final IGraphMsgBox<K, M> graphMsgBox;
    private final long maxIteration;
    protected long iterationId;
    protected K vertexId;

    public StaticGraphContextImpl(Operator.OpContext opContext,
                                  RuntimeContext runtimeContext,
                                  GraphState<K, VV, EV> graphState,
                                  IGraphMsgBox<K, M> graphMsgBox,
                                  long maxIteration) {
        this.opContext = opContext;
        this.runtimeContext = runtimeContext;
        this.graphState = graphState;
        this.graphMsgBox = graphMsgBox;
        this.maxIteration = maxIteration;
    }

    public void init(long iterationId, K vertexId) {
        this.iterationId = iterationId;
        this.vertexId = vertexId;
    }

    @Override
    public long getJobId() {
        return this.opContext.getRuntimeContext().getPipelineId();
    }

    @Override
    public long getIterationId() {
        return this.iterationId;
    }

    @Override
    public RuntimeContext getRuntimeContext() {
        return this.runtimeContext;
    }

    @Override
    public VertexCentricFunction.VertexQuery<K, VV> vertex() {
        return new StaticVertexQueryImpl<>(this.vertexId, this.graphState);
    }

    @Override
    public VertexCentricFunction.EdgeQuery<K, EV> edges() {
        return new StaticEdgeQueryImpl<>(this.vertexId, this.graphState);
    }

    @Override
    public void sendMessage(K vertexId, M message) {
        if (this.iterationId >= this.maxIteration) {
            return;
        }
        this.graphMsgBox.addOutMessage(vertexId, message);
    }

    @Override
    public void sendMessageToNeighbors(M message) {
        if (this.iterationId >= this.maxIteration) {
            return;
        }
        try (CloseableIterator<IEdge<K, EV>> edgeIterator
                 = this.graphState.staticGraph().E().query(this.vertexId).iterator()) {
            while (edgeIterator.hasNext()) {
                IEdge<K, EV> edge = edgeIterator.next();
                this.graphMsgBox.addOutMessage(edge.getTargetId(), message);
            }
        } catch (Exception e) {
            throw new GeaflowRuntimeException(e);
        }
    }

}
