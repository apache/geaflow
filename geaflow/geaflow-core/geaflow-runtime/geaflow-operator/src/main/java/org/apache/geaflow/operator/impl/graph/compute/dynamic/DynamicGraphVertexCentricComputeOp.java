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

package org.apache.geaflow.operator.impl.graph.compute.dynamic;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.geaflow.api.function.iterator.RichIteratorFunction;
import org.apache.geaflow.api.graph.base.algo.AbstractIncVertexCentricComputeAlgo;
import org.apache.geaflow.api.graph.function.vc.IncVertexCentricComputeFunction;
import org.apache.geaflow.api.graph.function.vc.IncVertexCentricComputeFunction.IncGraphComputeContext;
import org.apache.geaflow.api.graph.function.vc.base.IncGraphInferContext;
import org.apache.geaflow.collector.ICollector;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.common.config.keys.FrameworkConfigKeys;
import org.apache.geaflow.common.exception.GeaflowRuntimeException;
import org.apache.geaflow.infer.InferContext;
import org.apache.geaflow.infer.InferContextPool;
import org.apache.geaflow.model.graph.message.DefaultGraphMessage;
import org.apache.geaflow.model.graph.vertex.IVertex;
import org.apache.geaflow.model.record.RecordArgs.GraphRecordNames;
import org.apache.geaflow.operator.OpArgs;
import org.apache.geaflow.operator.OpArgs.OpType;
import org.apache.geaflow.operator.impl.graph.algo.vc.IGraphVertexCentricOp;
import org.apache.geaflow.operator.impl.graph.algo.vc.context.dynamic.IncGraphContextImpl;
import org.apache.geaflow.operator.impl.graph.algo.vc.msgbox.IGraphMsgBox.MsgProcessFunc;
import org.apache.geaflow.operator.impl.iterator.IteratorOperator;
import org.apache.geaflow.view.graph.GraphViewDesc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DynamicGraphVertexCentricComputeOp<K, VV, EV, M, FUNC extends IncVertexCentricComputeFunction<K, VV, EV, M>>
    extends AbstractDynamicGraphVertexCentricOp<K, VV, EV, M, AbstractIncVertexCentricComputeAlgo<K, VV, EV, M, FUNC>>
    implements IGraphVertexCentricOp<K, VV, EV, M>, IteratorOperator {

    private static final Logger LOGGER = LoggerFactory.getLogger(DynamicGraphVertexCentricComputeOp.class);

    protected IncGraphComputeContextImpl graphIncVCComputeCtx;
    protected IncVertexCentricComputeFunction<K, VV, EV, M> incVCComputeFunction;

    private Set<K> invokeVIds;

    private ICollector<IVertex<K, VV>> vertexCollector;

    protected Configuration configuration;

    public DynamicGraphVertexCentricComputeOp(GraphViewDesc graphViewDesc,
                                              AbstractIncVertexCentricComputeAlgo<K, VV, EV, M, FUNC> incVCAlgorithm) {
        super(graphViewDesc, incVCAlgorithm);
        opArgs.setOpType(OpType.INC_VERTEX_CENTRIC_COMPUTE);
        opArgs.setChainStrategy(OpArgs.ChainStrategy.NEVER);
    }

    @Override
    public void open(OpContext opContext) {
        super.open(opContext);
        this.incVCComputeFunction = this.function.getIncComputeFunction();
        this.configuration = runtimeContext.getConfiguration();
        this.graphIncVCComputeCtx = configuration.getBoolean(FrameworkConfigKeys.INFER_ENV_ENABLE)
            ? new IncGraphInferComputeContextImpl() : new IncGraphComputeContextImpl();
        this.incVCComputeFunction.init(this.graphIncVCComputeCtx);

        this.invokeVIds = new HashSet<>();

        for (ICollector collector : this.collectors) {
            if (!collector.getTag().equals(GraphRecordNames.Message.name())
                && !collector.getTag().equals(GraphRecordNames.Aggregate.name())) {
                vertexCollector = collector;
            }
        }
    }

    @Override
    public void doFinishIteration(long iterations) {
        LOGGER.info("finish iteration:{}", iterations);
        //compute
        if (this.iterations == 1L) {
            Set<K> vIds = temporaryGraphCache.getAllEvolveVId();
            this.invokeVIds.addAll(vIds);
            for (K vId : vIds) {
                this.graphIncVCComputeCtx.init(iterations, vId);
                this.incVCComputeFunction.evolve(vId,
                    this.graphIncVCComputeCtx.getTemporaryGraph());
            }
        } else {
            this.graphMsgBox.processInMessage(new MsgProcessFunc<K, M>() {
                @Override
                public void process(K vertexId, List<M> ms) {
                    graphIncVCComputeCtx.init(iterations, vertexId);
                    invokeVIds.add(vertexId);
                    incVCComputeFunction.compute(vertexId, ms.iterator());
                }
            });
            this.graphMsgBox.clearInBox();
        }
        if (incVCComputeFunction instanceof RichIteratorFunction) {
            ((RichIteratorFunction) incVCComputeFunction).finishIteration(iterations);
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

        this.messageCollector.finish();
        this.graphMsgBox.clearOutBox();
    }

    @Override
    public void finish() {
        LOGGER.info("current batch invokeIds:{}", this.invokeVIds);
        for (K vertexId : this.invokeVIds) {
            this.graphIncVCComputeCtx.init(iterations, vertexId);
            this.incVCComputeFunction.finish(vertexId, this.graphIncVCComputeCtx.getMutableGraph());
        }
        this.invokeVIds.clear();
        this.temporaryGraphCache.clear();
        vertexCollector.finish();
        checkpoint();
    }

    class IncGraphComputeContextImpl extends IncGraphContextImpl<K, VV, EV, M> implements IncGraphComputeContext<K, VV, EV, M> {

        public IncGraphComputeContextImpl() {
            super(opContext, runtimeContext, graphState, temporaryGraphCache, graphMsgBox, maxIterations);
        }

        @Override
        public void collect(IVertex vertex) {
            vertexCollector.partition(vertex.getId(), vertex);
        }
    }

    class IncGraphInferComputeContextImpl<OUT> extends IncGraphComputeContextImpl implements
        IncGraphInferContext<OUT> {

        private final ThreadLocal<InferContext> clientLocal = new ThreadLocal<>();

        private final InferContext<OUT> inferContext;

        public IncGraphInferComputeContextImpl() {
            if (clientLocal.get() == null) {
                try {
                    // Use InferContextPool instead of direct instantiation
                    // This ensures efficient reuse of InferContext instances
                    inferContext = InferContextPool.getOrCreate(runtimeContext.getConfiguration());
                    clientLocal.set(inferContext);
                    LOGGER.debug("InferContext obtained from pool: {}", 
                        InferContextPool.getStatus());
                } catch (Exception e) {
                    LOGGER.error("Failed to obtain InferContext from pool", e);
                    throw new GeaflowRuntimeException(
                        "InferContext initialization failed: " + e.getMessage(), e);
                }
            } else {
                inferContext = clientLocal.get();
            }
        }

        @Override
        public OUT infer(Object... modelInputs) {
            try {
                return inferContext.infer(modelInputs);
            } catch (Exception e) {
                throw new GeaflowRuntimeException("model infer failed", e);
            }
        }

        @Override
        public void close() throws IOException {
            if (clientLocal.get() != null) {
                // Do NOT close the InferContext here since it's managed by the pool
                // The pool handles lifecycle management
                LOGGER.debug("Detaching from pooled InferContext");
                clientLocal.remove();
            }
        }
    }
}
