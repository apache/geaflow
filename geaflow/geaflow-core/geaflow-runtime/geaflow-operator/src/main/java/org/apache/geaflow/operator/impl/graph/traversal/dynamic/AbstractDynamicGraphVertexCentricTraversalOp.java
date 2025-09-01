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

package org.apache.geaflow.operator.impl.graph.traversal.dynamic;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import org.apache.geaflow.api.function.iterator.RichIteratorFunction;
import org.apache.geaflow.api.graph.base.algo.AbstractIncVertexCentricTraversalAlgo;
import org.apache.geaflow.api.graph.function.vc.IncVertexCentricTraversalFunction;
import org.apache.geaflow.api.graph.function.vc.IncVertexCentricTraversalFunction.IncVertexCentricTraversalFuncContext;
import org.apache.geaflow.api.graph.function.vc.IncVertexCentricTraversalFunction.TraversalHistoricalGraph;
import org.apache.geaflow.collector.ICollector;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.common.config.keys.FrameworkConfigKeys;
import org.apache.geaflow.model.graph.message.DefaultGraphMessage;
import org.apache.geaflow.model.graph.message.IGraphMessage;
import org.apache.geaflow.model.record.RecordArgs.GraphRecordNames;
import org.apache.geaflow.model.traversal.ITraversalRequest;
import org.apache.geaflow.model.traversal.ITraversalResponse;
import org.apache.geaflow.operator.OpArgs.OpType;
import org.apache.geaflow.operator.impl.graph.algo.vc.IGraphTraversalOp;
import org.apache.geaflow.operator.impl.graph.algo.vc.context.dynamic.IncGraphContextImpl;
import org.apache.geaflow.operator.impl.graph.algo.vc.context.dynamic.IncHistoricalGraph;
import org.apache.geaflow.operator.impl.graph.algo.vc.context.dynamic.TraversalIncHistoricalGraph;
import org.apache.geaflow.operator.impl.graph.algo.vc.msgbox.IGraphMsgBox.MsgProcessFunc;
import org.apache.geaflow.operator.impl.graph.compute.dynamic.AbstractDynamicGraphVertexCentricOp;
import org.apache.geaflow.view.graph.GraphViewDesc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractDynamicGraphVertexCentricTraversalOp<K, VV, EV, M, R,
    FUNC extends IncVertexCentricTraversalFunction<K, VV, EV, M, R>>
    extends AbstractDynamicGraphVertexCentricOp<K, VV, EV, M,
    AbstractIncVertexCentricTraversalAlgo<K, VV, EV, M, R, FUNC>>
    implements IGraphTraversalOp<K, VV, EV, M> {

    private static final Logger LOGGER = LoggerFactory.getLogger(
        AbstractDynamicGraphVertexCentricTraversalOp.class);

    protected IncGraphVCTraversalCtxImpl graphVCTraversalCtx;
    protected IncVertexCentricTraversalFunction<K, VV, EV, M, R> incVcTraversalFunction;

    protected boolean addInvokeVIdsEachIteration = false;
    protected Set<K> invokeVIds;
    protected List<ITraversalResponse<R>> responses;

    protected ICollector<ITraversalResponse<R>> responseCollector;

    protected final List<ITraversalRequest<K>> traversalRequests;

    public AbstractDynamicGraphVertexCentricTraversalOp(
        GraphViewDesc graphViewDesc,
        AbstractIncVertexCentricTraversalAlgo<K, VV, EV, M, R, FUNC> incVertexCentricTraversal) {
        super(graphViewDesc, incVertexCentricTraversal);
        opArgs.setOpType(OpType.INC_VERTEX_CENTRIC_TRAVERSAL);
        this.traversalRequests = new ArrayList<>();
    }

    @Override
    public void open(OpContext opContext) {
        super.open(opContext);

        this.incVcTraversalFunction = this.function.getIncTraversalFunction();
        this.graphVCTraversalCtx = new IncGraphVCTraversalCtxImpl(getIdentify(), messageCollector);
        this.incVcTraversalFunction.open(this.graphVCTraversalCtx);

        this.addInvokeVIdsEachIteration = Configuration.getBoolean(FrameworkConfigKeys.ADD_INVOKE_VIDS_EACH_ITERATION,
                opContext.getRuntimeContext().getConfiguration().getConfigMap());
        this.invokeVIds = new HashSet<>();
        this.responses = new ArrayList<>();

        for (ICollector collector : this.collectors) {
            if (!collector.getTag().equals(GraphRecordNames.Message.name())
                && !collector.getTag().equals(GraphRecordNames.Aggregate.name())) {
                responseCollector = collector;
            }
        }
    }

    @Override
    public void doFinishIteration(long iterations) {
        // Compute.
        if (iterations == 1L) {
            // Evolve.
            Set<K> vIds = temporaryGraphCache.getAllEvolveVId();
            this.invokeVIds.addAll(vIds);
            for (K vId : vIds) {
                this.graphVCTraversalCtx.init(iterations, vId);
                this.incVcTraversalFunction.evolve(vId, this.graphVCTraversalCtx.getTemporaryGraph());
            }
            traversalByRequest();
        } else {
            this.graphMsgBox.processInMessage(new MsgProcessFunc<K, M>() {
                @Override
                public void process(K vertexId, List<M> messages) {
                    if (addInvokeVIdsEachIteration) {
                        invokeVIds.add(vertexId);
                    }
                    graphVCTraversalCtx.init(iterations, vertexId);
                    incVcTraversalFunction.compute(vertexId, messages.iterator());
                }
            });
            this.graphMsgBox.clearInBox();
        }
        if (this.incVcTraversalFunction instanceof RichIteratorFunction) {
            ((RichIteratorFunction) this.incVcTraversalFunction).finishIteration(iterations);
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

    protected void traversalByRequest() {
        Iterator<ITraversalRequest<K>> iterator = getTraversalRequests();
        while (iterator.hasNext()) {
            ITraversalRequest<K> traversalRequest = iterator.next();
            K vertexId = traversalRequest.getVId();
            this.graphVCTraversalCtx.init(iterations, vertexId);
            this.incVcTraversalFunction.init(traversalRequest);
        }
    }

    @Override
    public void finish() {
        LOGGER.info("current batch invokeIds size:{}", this.invokeVIds.size());
        for (K vertexId : this.invokeVIds) {
            this.graphVCTraversalCtx.init(iterations, vertexId);
            this.incVcTraversalFunction.finish(vertexId, this.graphVCTraversalCtx.getMutableGraph());
        }
        this.temporaryGraphCache.clear();
        this.invokeVIds.clear();
        this.traversalRequests.clear();

        LOGGER.info("incVcTraversalFunction finish, windowId:{}, invokeIds size:{}", this.windowId, this.invokeVIds.size());
        incVcTraversalFunction.finish();
        LOGGER.info("incVcTraversalFunction has finish windowId:{}", this.windowId);

        for (ITraversalResponse<R> response : this.responses) {
            responseCollector.partition(response.getResponseId(), response);
        }
        responseCollector.finish();
        responses.clear();
        checkpoint();
        LOGGER.info("TraversalOp has finish windowId:{}", this.windowId);
    }

    @Override
    public void close() {
        super.close();
        incVcTraversalFunction.close();
        this.responses.clear();
    }

    public class IncGraphVCTraversalCtxImpl extends IncGraphContextImpl<K, VV, EV, M>
        implements IncVertexCentricTraversalFuncContext<K, VV, EV, M, R> {

        private final ICollector<IGraphMessage<K, M>> messageCollector;
        private final String opName;
        private final TraversalHistoricalGraph<K, VV, EV> traversalHistoricalGraph;
        private boolean enableIncrMatch;

        protected IncGraphVCTraversalCtxImpl(String opName,
                                             ICollector<IGraphMessage<K, M>> messageCollector) {
            super(opContext, runtimeContext, graphState, temporaryGraphCache, graphMsgBox, maxIterations);
            this.opName = opName;
            this.messageCollector = messageCollector;
            this.traversalHistoricalGraph = new TraversalIncHistoricalGraph<>(
                (IncHistoricalGraph<K, VV, EV>) super.getHistoricalGraph());
        }

        public boolean isEnableIncrMatch() {
            return enableIncrMatch;
        }

        public IncGraphVCTraversalCtxImpl setEnableIncrMatch(boolean enableIncrMatch) {
            this.enableIncrMatch = enableIncrMatch;
            return this;
        }

        @Override
        public void activeRequest(ITraversalRequest<K> request) {

        }

        @Override
        public void takeResponse(ITraversalResponse<R> response) {
            responses.add(response);
        }

        @Override
        public void broadcast(IGraphMessage<K, M> message) {
            messageCollector.broadcast(message);
        }

        @Override
        public TraversalHistoricalGraph<K, VV, EV> getHistoricalGraph() {
            return traversalHistoricalGraph;
        }

        @Override
        public String getTraversalOpName() {
            return opName;
        }
    }

    public void addRequest(ITraversalRequest<K> request) {
        LOGGER.info("add request:{}", request);
        traversalRequests.add(request);
    }

    public Iterator<ITraversalRequest<K>> getTraversalRequests() {
        return traversalRequests.iterator();
    }
}
