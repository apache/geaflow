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

package org.apache.geaflow.dsl.runtime.function.graph.source;

import com.google.common.base.Preconditions;
import java.io.IOException;
import java.util.Iterator;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.geaflow.api.context.RuntimeContext;
import org.apache.geaflow.api.function.RichFunction;
import org.apache.geaflow.api.function.io.SourceFunction;
import org.apache.geaflow.api.window.IWindow;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.common.config.keys.DSLConfigKeys;
import org.apache.geaflow.common.config.keys.ExecutionConfigKeys;
import org.apache.geaflow.common.exception.GeaflowRuntimeException;
import org.apache.geaflow.dsl.runtime.traversal.data.IdOnlyRequest;
import org.apache.geaflow.state.GraphState;
import org.apache.geaflow.state.StateFactory;
import org.apache.geaflow.state.descriptor.GraphStateDescriptor;
import org.apache.geaflow.utils.keygroup.IKeyGroupAssigner;
import org.apache.geaflow.utils.keygroup.KeyGroup;
import org.apache.geaflow.utils.keygroup.KeyGroupAssignerFactory;
import org.apache.geaflow.utils.keygroup.KeyGroupAssignment;
import org.apache.geaflow.view.IViewDesc.BackendType;
import org.apache.geaflow.view.graph.GraphViewDesc;
import org.apache.geaflow.view.meta.ViewMetaBookKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractVertexScanSourceFunction<K> extends RichFunction implements
    SourceFunction<IdOnlyRequest> {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractVertexScanSourceFunction.class);

    protected transient RuntimeContext runtimeContext;

    protected GraphViewDesc graphViewDesc;

    protected transient GraphState<K, ?, ?> graphState;

    private Iterator<K> idIterator;

    private long windSize;

    private static final AtomicInteger storeCounter = new AtomicInteger(0);

    public AbstractVertexScanSourceFunction(GraphViewDesc graphViewDesc) {
        this.graphViewDesc = Objects.requireNonNull(graphViewDesc);
    }

    @Override
    public void open(RuntimeContext runtimeContext) {
        this.runtimeContext = runtimeContext;
        this.windSize = this.runtimeContext.getConfiguration().getLong(DSLConfigKeys.GEAFLOW_DSL_WINDOW_SIZE);
        Configuration rewriteConfiguration = runtimeContext.getConfiguration();
        String jobName = rewriteConfiguration.getString(ExecutionConfigKeys.JOB_APP_NAME);
        // A read-only graph copy will be created locally for the VertexScan.
        // To avoid conflicts with other VertexScans or Ops, an independent copy name is
        // constructed using the job name to differentiate the storage path.
        rewriteConfiguration.put(ExecutionConfigKeys.JOB_APP_NAME.getKey(),
            "VertexScanSourceFunction_" + jobName + "_" + storeCounter.getAndIncrement());
        GraphStateDescriptor<K, ?, ?> desc = buildGraphStateDesc();
        desc.withMetricGroup(runtimeContext.getMetric());
        this.graphState = StateFactory.buildGraphState(desc, runtimeContext.getConfiguration());
        recover();
        this.idIterator = buildIdIterator();
    }

    protected abstract Iterator<K> buildIdIterator();

    protected void recover() {
        LOGGER.info("Task: {} will do recover, windowId: {}",
            this.runtimeContext.getTaskArgs().getTaskId(), this.runtimeContext.getWindowId());
        long lastCheckPointId = getLatestViewVersion();
        if (lastCheckPointId >= 0) {
            LOGGER.info("Task: {} do recover to state VersionId: {}", this.runtimeContext.getTaskArgs().getTaskId(),
                lastCheckPointId);
            graphState.manage().operate().setCheckpointId(lastCheckPointId);
            graphState.manage().operate().recover();
        }
    }

    @Override
    public void init(int parallel, int index) {

    }

    protected GraphStateDescriptor<K, ?, ?> buildGraphStateDesc() {
        int taskIndex = runtimeContext.getTaskArgs().getTaskIndex();
        int taskPara = runtimeContext.getTaskArgs().getParallelism();
        BackendType backendType = graphViewDesc.getBackend();
        GraphStateDescriptor<K, ?, ?> desc = GraphStateDescriptor.build(graphViewDesc.getName()
            , backendType.name());

        int maxPara = graphViewDesc.getShardNum();
        Preconditions.checkArgument(taskPara <= maxPara,
            String.format("task parallelism '%s' must be <= shard num(max parallelism) '%s'",
                taskPara, maxPara));

        KeyGroup keyGroup = KeyGroupAssignment.computeKeyGroupRangeForOperatorIndex(maxPara, taskPara, taskIndex);
        IKeyGroupAssigner keyGroupAssigner =
            KeyGroupAssignerFactory.createKeyGroupAssigner(keyGroup, taskIndex, maxPara);
        desc.withKeyGroup(keyGroup);
        desc.withKeyGroupAssigner(keyGroupAssigner);

        long taskId = runtimeContext.getTaskArgs().getTaskId();
        int containerNum = runtimeContext.getConfiguration().getInteger(ExecutionConfigKeys.CONTAINER_NUM);
        LOGGER.info("Task:{} taskId:{} taskIndex:{} keyGroup:{} containerNum:{} real taskIndex:{}",
            this.runtimeContext.getTaskArgs().getTaskName(),
            taskId,
            taskIndex,
            desc.getKeyGroup(), containerNum, runtimeContext.getTaskArgs().getTaskIndex());
        return desc;
    }

    protected long getLatestViewVersion() {
        long lastCheckPointId;
        try {
            ViewMetaBookKeeper keeper = new ViewMetaBookKeeper(graphViewDesc.getName(),
                this.runtimeContext.getConfiguration());
            lastCheckPointId = keeper.getLatestViewVersion(graphViewDesc.getName());
            LOGGER.info("Task: {} will do recover or load, ViewMetaBookKeeper version: {}",
                runtimeContext.getTaskArgs().getTaskId(), lastCheckPointId);
        } catch (IOException e) {
            throw new GeaflowRuntimeException(e);
        }
        return lastCheckPointId;
    }

    @Override
    public boolean fetch(IWindow<IdOnlyRequest> window, SourceContext<IdOnlyRequest> ctx) throws Exception {
        int count = 0;
        while (idIterator.hasNext()) {
            K id = idIterator.next();
            IdOnlyRequest idOnlyRequest = new IdOnlyRequest(id);
            ctx.collect(idOnlyRequest);
            count++;
            if (count == windSize) {
                break;
            }
        }
        return count == windSize;
    }

    @Override
    public void close() {

    }
}
