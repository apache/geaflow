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

package org.apache.geaflow.operator.impl.graph.algo.vc;

import com.google.common.base.Preconditions;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.geaflow.api.function.iterator.RichIteratorFunction;
import org.apache.geaflow.api.graph.base.algo.VertexCentricAlgo;
import org.apache.geaflow.api.graph.function.vc.VertexCentricCombineFunction;
import org.apache.geaflow.api.partition.graph.edge.IGraphVCPartition;
import org.apache.geaflow.collector.AbstractCollector;
import org.apache.geaflow.collector.ICollector;
import org.apache.geaflow.common.config.keys.ExecutionConfigKeys;
import org.apache.geaflow.common.config.keys.FrameworkConfigKeys;
import org.apache.geaflow.common.exception.GeaflowRuntimeException;
import org.apache.geaflow.context.AbstractRuntimeContext;
import org.apache.geaflow.metrics.common.MetricNameFormatter;
import org.apache.geaflow.metrics.common.api.Meter;
import org.apache.geaflow.model.graph.message.IGraphMessage;
import org.apache.geaflow.model.record.RecordArgs.GraphRecordNames;
import org.apache.geaflow.operator.OpArgs;
import org.apache.geaflow.operator.base.AbstractOperator;
import org.apache.geaflow.operator.impl.graph.algo.vc.msgbox.GraphMsgBoxFactory;
import org.apache.geaflow.operator.impl.graph.algo.vc.msgbox.IGraphMsgBox;
import org.apache.geaflow.operator.impl.iterator.IteratorOperator;
import org.apache.geaflow.state.GraphState;
import org.apache.geaflow.state.StateFactory;
import org.apache.geaflow.state.descriptor.GraphStateDescriptor;
import org.apache.geaflow.state.graph.StateMode;
import org.apache.geaflow.state.manage.LoadOption;
import org.apache.geaflow.utils.keygroup.IKeyGroupAssigner;
import org.apache.geaflow.utils.keygroup.KeyGroup;
import org.apache.geaflow.utils.keygroup.KeyGroupAssignerFactory;
import org.apache.geaflow.utils.keygroup.KeyGroupAssignment;
import org.apache.geaflow.view.IViewDesc.BackendType;
import org.apache.geaflow.view.graph.GraphViewDesc;
import org.apache.geaflow.view.meta.ViewMetaBookKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractGraphVertexCentricOp<K, VV, EV, M,
    FUNC extends VertexCentricAlgo<K, VV, EV, M>> extends
    AbstractOperator<FUNC> implements IGraphVertexCentricOp<K, VV, EV, M>, IteratorOperator {

    private static final Logger LOGGER = LoggerFactory.getLogger(
        AbstractGraphVertexCentricOp.class);

    protected int taskId;

    protected VertexCentricCombineFunction<M> msgCombineFunction;
    protected IGraphVCPartition<K> graphVCPartition;
    protected final GraphViewDesc graphViewDesc;

    protected long maxIterations;
    protected long iterations;
    protected long windowId;

    protected KeyGroup keyGroup;
    protected KeyGroup taskKeyGroup;
    protected GraphState<K, VV, EV> graphState;
    protected IGraphMsgBox<K, M> graphMsgBox;
    protected boolean shareEnable;

    protected Map<String, ICollector> collectorMap;
    protected ICollector<IGraphMessage<K, M>> messageCollector;
    protected Meter msgMeter;

    public AbstractGraphVertexCentricOp(GraphViewDesc graphViewDesc, FUNC func) {
        super(func);
        this.graphViewDesc = graphViewDesc;
        this.maxIterations = func.getMaxIterationCount();
        opArgs.setChainStrategy(OpArgs.ChainStrategy.NEVER);
    }

    @Override
    public void open(OpContext opContext) {
        super.open(opContext);

        this.msgCombineFunction = function.getCombineFunction();
        this.graphVCPartition = function.getGraphPartition();
        this.windowId = runtimeContext.getWindowId();
        this.msgMeter = this.metricGroup.meter(
            MetricNameFormatter.iterationMsgMetricName(this.getClass(), this.opArgs.getOpId()));

        shareEnable = runtimeContext.getConfiguration().getBoolean(FrameworkConfigKeys.SERVICE_SHARE_ENABLE);

        GraphStateDescriptor<K, VV, EV> desc = buildGraphStateDesc(opArgs.getOpName());
        desc.withMetricGroup(runtimeContext.getMetric());
        this.graphState = StateFactory.buildGraphState(desc, runtimeContext.getConfiguration());
        LOGGER.info("ThreadId {}, open graphState", Thread.currentThread().getId());
        if (!shareEnable) {
            this.taskKeyGroup = keyGroup;
            LOGGER.info("recovery graph state {}", graphState);
            recover();
        } else {
            load();
            LOGGER.info("processIndex {} taskIndex {} load shard {}, load graph state {}",
                runtimeContext.getTaskArgs().getProcessIndex(), runtimeContext.getTaskArgs().getTaskIndex(), keyGroup, graphState);
        }

        collectorMap = new HashMap<>();
        for (ICollector collector : this.collectors) {
            collectorMap.put(collector.getTag(), collector);
        }
        this.messageCollector = collectorMap.get(GraphRecordNames.Message.name());
        if (this.messageCollector instanceof AbstractCollector) {
            ((AbstractCollector) this.messageCollector).setOutputMetric(this.msgMeter);
        }
        this.graphMsgBox = GraphMsgBoxFactory.buildMessageBox(this.messageCollector, this.msgCombineFunction);
    }

    protected GraphStateDescriptor<K, VV, EV> buildGraphStateDesc(String name) {
        this.taskId = runtimeContext.getTaskArgs().getTaskId();

        int containerNum = runtimeContext.getConfiguration().getInteger(ExecutionConfigKeys.CONTAINER_NUM);
        int processIndex = runtimeContext.getTaskArgs().getProcessIndex();
        int taskIndex = shareEnable ? processIndex : runtimeContext.getTaskArgs().getTaskIndex();
        int taskPara = shareEnable ? containerNum : runtimeContext.getTaskArgs().getParallelism();
        BackendType backendType = graphViewDesc.getBackend();
        GraphStateDescriptor<K, VV, EV> desc = GraphStateDescriptor.build(graphViewDesc.getName()
            , backendType.name());

        int maxPara = graphViewDesc.getShardNum();
        Preconditions.checkArgument(taskPara <= maxPara,
            String.format("task parallelism '%s' must be <= shard num(max parallelism) '%s'",
                taskPara, maxPara));

        keyGroup = KeyGroupAssignment.computeKeyGroupRangeForOperatorIndex(maxPara, taskPara, taskIndex);
        IKeyGroupAssigner keyGroupAssigner =
            KeyGroupAssignerFactory.createKeyGroupAssigner(keyGroup, taskIndex, maxPara);
        desc.withKeyGroup(keyGroup);
        desc.withKeyGroupAssigner(keyGroupAssigner);
        if (shareEnable) {
            LOGGER.info("enable state singleton");
            desc.withSingleton();
            desc.withStateMode(StateMode.RDONLY);
        }
        LOGGER.info("opName:{} taskId:{} taskIndex:{} keyGroup:{} containerNum:{} processIndex: {} real taskIndex:{}", this.opArgs.getOpName(),
            taskId,
            taskIndex,
            desc.getKeyGroup(), containerNum, processIndex, runtimeContext.getTaskArgs().getTaskIndex());
        return desc;
    }

    @Override
    public void processMessage(IGraphMessage<K, M> graphMessage) {
        if (enableDebug) {
            LOGGER.info("taskId:{} windowId:{} Iteration:{} add message:{}", taskId, windowId,
                iterations,
                graphMessage);
        }
        K vertexId = graphMessage.getTargetVId();
        while (graphMessage.hasNext()) {
            this.graphMsgBox.addInMessages(vertexId, graphMessage.next());
        }
        this.opInputMeter.mark();
    }

    @Override
    public void initIteration(long iterations) {
        this.iterations = iterations;
        this.windowId = opContext.getRuntimeContext().getWindowId();
        ((AbstractRuntimeContext) this.runtimeContext).updateWindowId(windowId);
        if (enableDebug) {
            LOGGER.info("taskId:{} windowId:{} init Iteration:{}", taskId, windowId, iterations);
        }
        this.iterations = iterations;
        if (function instanceof RichIteratorFunction) {
            ((RichIteratorFunction) function).initIteration(iterations);
        }
    }

    @Override
    public long getMaxIterationCount() {
        return this.maxIterations;
    }

    @Override
    public void finishIteration(long iteration) {
        this.ticToc.tic();
        this.doFinishIteration(iteration);
        this.metricGroup.histogram(
            MetricNameFormatter.iterationFinishMetricName(this.getClass(), this.opArgs.getOpId(), iteration)
        ).update(this.ticToc.toc());
    }

    public abstract void doFinishIteration(long iteration);

    @Override
    public void close() {
        this.graphMsgBox.clearInBox();
        this.graphMsgBox.clearOutBox();
        if (!shareEnable) {
            this.graphState.manage().operate().close();
        }
    }

    protected void recover() {
        LOGGER.info("opName: {} will do recover, windowId: {}", this.opArgs.getOpName(), this.windowId);
        long lastCheckPointId = getLatestViewVersion();
        if (lastCheckPointId >= 0) {
            LOGGER.info("opName: {} do recover to state VersionId: {}", this.opArgs.getOpName(),
                lastCheckPointId);
            graphState.manage().operate().setCheckpointId(lastCheckPointId);
            graphState.manage().operate().recover();
        }
    }

    public String getGraphViewName() {
        return graphViewDesc.getName();
    }

    protected void load() {
        LOGGER.info("opName: {} will do load, windowId: {}", this.opArgs.getOpName(), this.windowId);
        long lastCheckPointId = getLatestViewVersion();
        long checkPointId = lastCheckPointId < 0 ? 0 : lastCheckPointId;
        LOGGER.info("opName: {} do load, ViewMetaBookKeeper version: {}, checkPointId {}",
            this.opArgs.getOpName(), lastCheckPointId, checkPointId);

        LoadOption loadOption = LoadOption.of();
        this.taskKeyGroup = KeyGroupAssignment.computeKeyGroupRangeForOperatorIndex(
            graphViewDesc.getShardNum(),
            runtimeContext.getTaskArgs().getParallelism(),
            runtimeContext.getTaskArgs().getTaskIndex());
        loadOption.withKeyGroup(this.taskKeyGroup);
        loadOption.withCheckpointId(checkPointId);
        graphState.manage().operate().load(loadOption);
        LOGGER.info("opName: {} task key group {} do load successfully", this.opArgs.getOpName(), this.taskKeyGroup);
    }

    private long getLatestViewVersion() {
        long lastCheckPointId;
        try {
            ViewMetaBookKeeper keeper = new ViewMetaBookKeeper(graphViewDesc.getName(),
                this.runtimeContext.getConfiguration());
            lastCheckPointId = keeper.getLatestViewVersion(graphViewDesc.getName());
            LOGGER.info("opName: {} will do recover or load, ViewMetaBookKeeper version: {}",
                this.opArgs.getOpName(), lastCheckPointId);
        } catch (IOException e) {
            throw new GeaflowRuntimeException(e);
        }
        return lastCheckPointId;
    }

}
