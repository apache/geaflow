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

package org.apache.geaflow.runtime.core.scheduler.context;

import static org.apache.geaflow.common.config.keys.FrameworkConfigKeys.BATCH_NUMBER_PER_CHECKPOINT;
import static org.apache.geaflow.ha.runtime.HighAvailableLevel.CHECKPOINT;

import com.google.common.base.Preconditions;
import java.util.function.Supplier;
import org.apache.geaflow.cluster.common.IReliableContext;
import org.apache.geaflow.cluster.system.ClusterMetaStore;
import org.apache.geaflow.common.exception.GeaflowRuntimeException;
import org.apache.geaflow.common.utils.CheckpointUtil;
import org.apache.geaflow.ha.runtime.HighAvailableLevel;
import org.apache.geaflow.runtime.core.scheduler.cycle.IExecutionCycle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CheckpointSchedulerContext<
    C extends IExecutionCycle,
    PC extends IExecutionCycle,
    PCC extends ICycleSchedulerContext<PC, ?, ?>> extends AbstractCycleSchedulerContext<C, PC, PCC> implements IReliableContext {

    private static final Logger LOGGER = LoggerFactory.getLogger(CheckpointSchedulerContext.class);

    private final long checkpointDuration;
    private boolean isNeedFullCheckpoint = true;
    private transient long currentCheckpointId;
    private transient boolean isRecovered = false;

    public CheckpointSchedulerContext(C cycle, PCC parentContext) {
        super(cycle, parentContext);
        this.checkpointDuration = getConfig().getLong(BATCH_NUMBER_PER_CHECKPOINT);
        if (parentContext != null) {
            this.callbackFunction = ((AbstractCycleSchedulerContext<?, ?, ?>) parentContext).callbackFunction;
            ((AbstractCycleSchedulerContext<?, ?, ?>) parentContext).setCallbackFunction(null);
        }
    }

    @Override
    public void init() {
        load();
        if (!isRecovered) {
            checkpoint(new CycleCheckpointFunction());
        }
    }

    @Override
    public void checkpoint(long iterationId) {
        this.currentCheckpointId = iterationId;
        if (isNeedFullCheckpoint) {
            checkpoint(new CycleCheckpointFunction());
            isNeedFullCheckpoint = false;
        }
        if (CheckpointUtil.needDoCheckpoint(iterationId, checkpointDuration)) {
            checkpoint(new IterationIdCheckpointFunction());
            lastCheckpointId = iterationId;
        }
    }

    @Override
    public void load() {
        long windowId = loadWindowId(cycle.getPipelineTaskId());
        init(windowId);
        if (!isRecovered && windowId != CheckpointSchedulerContext.DEFAULT_INITIAL_ITERATION_ID) {
            setRollback(true);
        }
    }

    /**
     * Load cycle if exists, otherwise rebuild one by input func.
     */
    public static AbstractCycleSchedulerContext build(long pipelineTaskId, Supplier<? extends ICycleSchedulerContext> builder) {
        AbstractCycleSchedulerContext context = loadCycle(pipelineTaskId);
        if (context == null) {
            Preconditions.checkArgument(builder != null, "should provide function to build new context");
            context = (AbstractCycleSchedulerContext) builder.get();
            if (context == null) {
                throw new GeaflowRuntimeException("build new context failed");
            }
        }
        return context;
    }

    private static long loadWindowId(long pipelineTaskId) {
        Long lastWindowId = ClusterMetaStore.getInstance().getWindowId(pipelineTaskId);
        long windowId;
        if (lastWindowId == null) {
            windowId = CheckpointSchedulerContext.DEFAULT_INITIAL_ITERATION_ID;
            LOGGER.info("not found last success batchId, set startIterationId to {}", windowId);
        } else {
            // driver fo recover windowId
            windowId = lastWindowId + 1;
            LOGGER.info("load scheduler context, lastWindowId {}, current start windowId {}",
                lastWindowId, windowId);
        }
        return windowId;
    }

    private static CheckpointSchedulerContext<?, ?, ?> loadCycle(long pipelineTaskId) {
        CheckpointSchedulerContext context = (CheckpointSchedulerContext) ClusterMetaStore.getInstance().getCycle(pipelineTaskId);
        if (context == null) {
            LOGGER.info("not found recoverable cycle");
            return null;
        }
        context.isRecovered = true;
        context.init();
        return context;
    }

    @Override
    public void checkpoint(IReliableContextCheckpointFunction function) {
        function.doCheckpoint(this);
    }

    @Override
    public void close(IExecutionCycle cycle) {
        super.close(cycle);
    }

    @Override
    protected HighAvailableLevel getHaLevel() {
        return CHECKPOINT;
    }

    public boolean isRecovered() {
        return isRecovered;
    }

    public void setRecovered(boolean isRecovered) {
        this.isRecovered = isRecovered;
    }

    public class IterationIdCheckpointFunction implements IReliableContextCheckpointFunction {

        @Override
        public void doCheckpoint(IReliableContext context) {
            long checkpointId = ((CheckpointSchedulerContext) context).currentCheckpointId;
            ClusterMetaStore.getInstance().saveWindowId(checkpointId,
                (((CheckpointSchedulerContext) context).getCycle().getPipelineTaskId()));
            LOGGER.info("cycle {} do checkpoint {}",
                ((CheckpointSchedulerContext) context).getCycle().getCycleId(), checkpointId);
        }
    }

    public class CycleCheckpointFunction implements IReliableContextCheckpointFunction {

        @Override
        public void doCheckpoint(IReliableContext context) {
            long checkpointId = ((CheckpointSchedulerContext) context).currentCheckpointId;
            ClusterMetaStore.getInstance().saveCycle(context,
                ((CheckpointSchedulerContext) context).getCycle().getPipelineTaskId()).flush();
            LOGGER.info("cycle {} do checkpoint {} for full context",
                ((CheckpointSchedulerContext) context).getCycle().getCycleId(), checkpointId);
        }
    }
}
