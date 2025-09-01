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

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import org.apache.geaflow.cluster.resourcemanager.WorkerInfo;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.runtime.core.scheduler.ExecutableEventIterator.ExecutableEvent;
import org.apache.geaflow.runtime.core.scheduler.cycle.IExecutionCycle;
import org.apache.geaflow.runtime.core.scheduler.io.CycleResultManager;
import org.apache.geaflow.runtime.core.scheduler.resource.IScheduledWorkerManager;

public interface ICycleSchedulerContext<
    C extends IExecutionCycle,
    PC extends IExecutionCycle,
    PCC extends ICycleSchedulerContext<PC, ?, ?>> extends Serializable {

    /**
     * Returns execution cycle.
     */
    C getCycle();

    PCC getParentContext();

    /**
     * Returns execution config.
     */
    Configuration getConfig();

    /**
     * Returns whether cycle is finished.
     */
    boolean isCycleFinished();

    /**
     * Returns when cycle is recovered.
     */
    boolean isRecovered();

    /**
     * Returns whether cycle need rollback.
     */
    boolean isRollback();

    /**
     * Returns whether enable prefetch.
     */
    boolean isPrefetch();

    /**
     * Returns current iteration id.
     */
    long getCurrentIterationId();

    /**
     * Returns finish iteration id.
     */
    long getFinishIterationId();

    /**
     * Check whether has next iteration.
     */
    boolean hasNextIteration();

    /**
     * Returns next iteration id.
     */
    long getNextIterationId();

    /**
     * Check whether has next cycle to finish.
     */
    boolean hasNextToFinish();

    /**
     * Returns next finish iteration id.
     */
    long getNextFinishIterationId();

    /**
     * Returns initial iteration id.
     */
    long getInitialIterationId();

    /**
     * Returns cycle result manager.
     */
    CycleResultManager getResultManager();

    /**
     * Assign workers for cycle.
     */
    List<WorkerInfo> assign(C cycle);

    /**
     * Release worker for cycle.
     */
    void release(C cycle);

    /**
     * Finish the windowId iteration.
     */
    void finish(long windowId);

    /**
     * Finish cycle.
     */
    void finish();

    /**
     * Close workerManager.
     */
    void close(IExecutionCycle cycle);

    /**
     * Returns scheduler worker manager.
     */
    IScheduledWorkerManager<C> getSchedulerWorkerManager();

    /**
     * Returns prefetch events needed to be finished.
     */
    Map<Integer, ExecutableEvent> getPrefetchEvents();

    enum SchedulerState {
        /**
         * Init state.
         */
        INIT,
        /**
         * Execute state.
         */
        EXECUTE,
        /**
         * Finish state.
         */
        FINISH,
        /**
         * Rollback state.
         */
        ROLLBACK,
    }

}
