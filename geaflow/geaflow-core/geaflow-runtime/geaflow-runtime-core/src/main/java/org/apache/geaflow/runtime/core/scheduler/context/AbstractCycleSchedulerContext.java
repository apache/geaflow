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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.geaflow.cluster.resourcemanager.WorkerInfo;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.common.config.keys.ExecutionConfigKeys;
import org.apache.geaflow.ha.runtime.HighAvailableLevel;
import org.apache.geaflow.pipeline.callback.ICallbackFunction;
import org.apache.geaflow.runtime.core.scheduler.ExecutableEventIterator.ExecutableEvent;
import org.apache.geaflow.runtime.core.scheduler.cycle.IExecutionCycle;
import org.apache.geaflow.runtime.core.scheduler.io.CycleResultManager;
import org.apache.geaflow.runtime.core.scheduler.resource.IScheduledWorkerManager;
import org.apache.geaflow.runtime.core.scheduler.resource.ScheduledWorkerManagerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractCycleSchedulerContext<
    C extends IExecutionCycle, PC extends IExecutionCycle, PCC extends ICycleSchedulerContext<PC, ?, ?>>
    implements ICycleSchedulerContext<C, PC, PCC> {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractCycleSchedulerContext.class);

    public static final long DEFAULT_INITIAL_ITERATION_ID = 1;

    protected final C cycle;
    protected transient AtomicLong iterationIdGenerator;
    protected transient Queue<Long> flyingIterations;
    protected transient long currentIterationId;
    protected long finishIterationId;
    protected transient long initialIterationId;
    protected transient long lastCheckpointId;
    protected transient long terminateIterationId;

    protected transient PCC parentContext;
    protected IScheduledWorkerManager<C> workerManager;
    protected transient CycleResultManager cycleResultManager;
    protected ICallbackFunction callbackFunction;
    protected static ThreadLocal<Boolean> rollback = ThreadLocal.withInitial(() -> false);
    protected transient Map<Integer, ExecutableEvent> prefetchEvents;
    protected transient boolean prefetch;

    public AbstractCycleSchedulerContext(C cycle, PCC parentContext) {
        this.cycle = cycle;
        this.parentContext = parentContext;

        if (parentContext != null) {
            // Get worker manager from parent context, no need to init.
            this.workerManager = (IScheduledWorkerManager<C>) parentContext.getSchedulerWorkerManager();
            this.finishIterationId = cycle.getIterationCount() == Long.MAX_VALUE
                ? cycle.getIterationCount() : cycle.getIterationCount() + parentContext.getCurrentIterationId() - 1;
        } else {
            this.workerManager = (IScheduledWorkerManager<C>)
                ScheduledWorkerManagerFactory.createScheduledWorkerManager(
                    cycle.getConfig(),
                    ScheduledWorkerManagerFactory.getWorkerManagerHALevel(cycle)
                );
            this.finishIterationId = cycle.getIterationCount() == Long.MAX_VALUE
                ? cycle.getIterationCount() : cycle.getIterationCount() + DEFAULT_INITIAL_ITERATION_ID - 1;
        }
    }

    public void init() {
        long startIterationId;
        if (parentContext != null) {
            startIterationId = parentContext.getCurrentIterationId();
        } else {
            startIterationId = DEFAULT_INITIAL_ITERATION_ID;
        }
        init(startIterationId);
    }

    public void init(long startIterationId) {
        this.flyingIterations = new LinkedBlockingQueue<>(cycle.getFlyingCount());
        this.currentIterationId = startIterationId;
        this.initialIterationId = startIterationId;
        this.iterationIdGenerator = new AtomicLong(currentIterationId);
        this.lastCheckpointId = 0;
        this.terminateIterationId = Long.MAX_VALUE;
        if (parentContext != null) {
            this.cycleResultManager = parentContext.getResultManager();
        } else {
            this.cycleResultManager = new CycleResultManager();
        }
        prefetch = cycle.getConfig().getBoolean(ExecutionConfigKeys.SHUFFLE_PREFETCH);
        prefetchEvents = new HashMap<>();

        LOGGER.info("{} init cycle context onTheFlyThreshold {}, currentIterationId {}, "
                + "iterationCount {}, finishIterationId {}, initialIterationId {}",
            cycle.getPipelineName(), cycle.getFlyingCount(), this.currentIterationId,
            cycle.getIterationCount(), this.finishIterationId, this.initialIterationId);

    }

    @Override
    public C getCycle() {
        return this.cycle;
    }

    @Override
    public Configuration getConfig() {
        return cycle.getConfig();
    }

    @Override
    public boolean isCycleFinished() {
        return (iterationIdGenerator.get() > finishIterationId || lastCheckpointId >= terminateIterationId)
            && flyingIterations.isEmpty();
    }

    @Override
    public long getCurrentIterationId() {
        return currentIterationId;
    }

    public void setCurrentIterationId(long iterationId) {
        this.currentIterationId = iterationId;
    }

    @Override
    public boolean isRecovered() {
        return false;
    }

    @Override
    public boolean isRollback() {
        return rollback.get();
    }

    @Override
    public boolean isPrefetch() {
        return prefetch;
    }

    public void setRollback(boolean bool) {
        rollback.set(bool);
    }

    @Override
    public long getFinishIterationId() {
        return finishIterationId;
    }

    public void setFinishIterationId(long finishIterationId) {
        this.finishIterationId = finishIterationId;
    }

    @Override
    public boolean hasNextIteration() {
        return iterationIdGenerator.get() <= finishIterationId && lastCheckpointId < terminateIterationId
            && flyingIterations.size() < cycle.getFlyingCount();
    }

    @Override
    public long getNextIterationId() {
        long iterationId = iterationIdGenerator.getAndIncrement();
        flyingIterations.add(iterationId);
        this.currentIterationId = iterationId;
        return iterationId;
    }

    @Override
    public boolean hasNextToFinish() {
        return !flyingIterations.isEmpty() && !hasNextIteration();
    }

    @Override
    public long getNextFinishIterationId() {
        return flyingIterations.remove();
    }

    @Override
    public long getInitialIterationId() {
        return initialIterationId;
    }

    @Override
    public IScheduledWorkerManager<C> getSchedulerWorkerManager() {
        return workerManager;
    }

    @Override
    public CycleResultManager getResultManager() {
        return cycleResultManager;
    }

    @Override
    public PCC getParentContext() {
        return (PCC) this.parentContext;
    }

    public void setTerminateIterationId(long iterationId) {
        terminateIterationId = iterationId;
    }

    public void setCallbackFunction(ICallbackFunction callbackFunction) {
        this.callbackFunction = callbackFunction;
    }

    public Map<Integer, ExecutableEvent> getPrefetchEvents() {
        return this.prefetchEvents;
    }

    @Override
    public void finish(long windowId) {
        if (callbackFunction != null) {
            callbackFunction.window(windowId);
        }
        checkpoint(windowId);
    }
    
    @Override
    public void finish() {
        if (callbackFunction != null) {
            callbackFunction.terminal();
        }
    }

    @Override
    public List<WorkerInfo> assign(C cycle) {
        return workerManager.assign(cycle);
    }

    @Override
    public void release(C cycle) {
        workerManager.release(cycle);
    }

    @Override
    public void close(IExecutionCycle cycle) {
        workerManager.close(cycle);
    }

    abstract void checkpoint(long windowId);

    abstract HighAvailableLevel getHaLevel();

}
