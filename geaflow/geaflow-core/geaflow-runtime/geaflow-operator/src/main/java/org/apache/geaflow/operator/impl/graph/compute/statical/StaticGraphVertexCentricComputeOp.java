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

package org.apache.geaflow.operator.impl.graph.compute.statical;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import org.apache.geaflow.api.context.RuntimeContext;
import org.apache.geaflow.api.function.iterator.RichIteratorFunction;
import org.apache.geaflow.api.graph.base.algo.AbstractVertexCentricComputeAlgo;
import org.apache.geaflow.api.graph.function.vc.VertexCentricComputeFunction;
import org.apache.geaflow.api.graph.function.vc.VertexCentricComputeFunction.VertexCentricComputeFuncContext;
import org.apache.geaflow.collector.ICollector;
import org.apache.geaflow.common.exception.GeaflowRuntimeException;
import org.apache.geaflow.common.iterator.CloseableIterator;
import org.apache.geaflow.model.graph.message.DefaultGraphMessage;
import org.apache.geaflow.model.graph.vertex.IVertex;
import org.apache.geaflow.model.record.RecordArgs.GraphRecordNames;
import org.apache.geaflow.operator.OpArgs.OpType;
import org.apache.geaflow.operator.impl.graph.algo.vc.context.statical.StaticGraphContextImpl;
import org.apache.geaflow.operator.impl.graph.algo.vc.msgbox.IGraphMsgBox;
import org.apache.geaflow.operator.impl.graph.algo.vc.msgbox.IGraphMsgBox.MsgProcessFunc;
import org.apache.geaflow.state.GraphState;
import org.apache.geaflow.view.graph.GraphViewDesc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StaticGraphVertexCentricComputeOp<K, VV, EV, M, FUNC extends VertexCentricComputeFunction<K, VV, EV, M>>
    extends AbstractStaticGraphVertexCentricOp<K, VV, EV, M, AbstractVertexCentricComputeAlgo<K, VV, EV, M, FUNC>> {

    private static final Logger LOGGER = LoggerFactory.getLogger(StaticGraphVertexCentricComputeOp.class);

    protected GraphVCComputeCtxImpl graphVCComputeCtx;
    protected VertexCentricComputeFunction<K, VV, EV, M> vcComputeFunction;

    private ICollector<IVertex<K, VV>> vertexCollector;

    public StaticGraphVertexCentricComputeOp(GraphViewDesc graphViewDesc,
                                             AbstractVertexCentricComputeAlgo<K, VV, EV, M, FUNC> vcAlgorithm) {
        super(graphViewDesc, vcAlgorithm);
        opArgs.setOpType(OpType.VERTEX_CENTRIC_COMPUTE);
    }

    @Override
    public void open(OpContext opContext) {
        super.open(opContext);

        this.vcComputeFunction = this.function.getComputeFunction();

        this.graphVCComputeCtx = new GraphVCComputeCtxImpl(
            opContext, this.runtimeContext, this.graphState, this.graphMsgBox, this.maxIterations);
        this.vcComputeFunction.init(this.graphVCComputeCtx);

        for (ICollector collector : this.collectors) {
            if (!collector.getTag().equals(GraphRecordNames.Message.name())
                && !collector.getTag().equals(GraphRecordNames.Aggregate.name())) {
                vertexCollector = collector;
            }
        }

    }

    @Override
    public void doFinishIteration(long iterations) {

        // Compute.
        if (iterations == 1L) {
            Iterator<IVertex<K, VV>> vertexIterator = this.graphState.staticGraph().V().iterator();
            while (vertexIterator.hasNext()) {
                IVertex<K, VV> vertex = vertexIterator.next();
                K vertexId = vertex.getId();
                graphVCComputeCtx.init(iterations, vertexId);
                vcComputeFunction.compute(vertexId, Collections.emptyIterator());
            }
        } else {
            this.graphMsgBox.processInMessage(new MsgProcessFunc<K, M>() {
                @Override
                public void process(K vertexId, List<M> ms) {
                    graphVCComputeCtx.init(iterations, vertexId);
                    vcComputeFunction.compute(vertexId, ms.iterator());
                }
            });
            this.graphMsgBox.clearInBox();
        }
        if (vcComputeFunction instanceof RichIteratorFunction) {
            ((RichIteratorFunction) vcComputeFunction).finishIteration(iterations);
        }
        // Emit message.
        this.graphMsgBox.processOutMessage(new MsgProcessFunc<K, M>() {
            @Override
            public void process(K vertexId, List<M> messages) {
                // Collect message.
                int size = messages.size();
                for (int i = 0; i < size; i++) {
                    messageCollector.partition(vertexId, new DefaultGraphMessage<>(vertexId, messages.get(i)));
                }
            }
        });
        messageCollector.finish();
        this.graphMsgBox.clearOutBox();
    }


    @Override
    public void finish() {
        try (CloseableIterator<IVertex<K, VV>> vertexIterator = graphState.staticGraph().V().query().iterator()) {
            while (vertexIterator.hasNext()) {
                IVertex<K, VV> vertex = vertexIterator.next();
                vertexCollector.partition(vertex.getId(), vertex);
            }
        } catch (Exception e) {
            throw new GeaflowRuntimeException(e);
        }
        this.vcComputeFunction.finish();
        vertexCollector.finish();
    }

    class GraphVCComputeCtxImpl extends StaticGraphContextImpl<K, VV, EV, M> implements VertexCentricComputeFuncContext<K, VV, EV, M> {

        public GraphVCComputeCtxImpl(OpContext opContext,
                                     RuntimeContext runtimeContext,
                                     GraphState<K, VV, EV> graphState,
                                     IGraphMsgBox<K, M> graphMsgBox,
                                     long maxIteration) {
            super(opContext, runtimeContext, graphState, graphMsgBox, maxIteration);
        }

        @Override
        public void setNewVertexValue(VV value) {
            IVertex<K, VV> valueVertex = graphState.staticGraph().V().query(vertexId).get();
            valueVertex = valueVertex.withValue(value);
            graphState.staticGraph().V().add(valueVertex);
        }

    }

}
