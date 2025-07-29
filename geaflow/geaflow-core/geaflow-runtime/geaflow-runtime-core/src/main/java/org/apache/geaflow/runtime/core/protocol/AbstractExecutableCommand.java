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

package org.apache.geaflow.runtime.core.protocol;

import org.apache.geaflow.cluster.collector.EmitterRunner;
import org.apache.geaflow.cluster.fetcher.FetcherRunner;
import org.apache.geaflow.cluster.protocol.EventType;
import org.apache.geaflow.cluster.protocol.IExecutableCommand;
import org.apache.geaflow.cluster.rpc.RpcClient;
import org.apache.geaflow.cluster.task.ITaskContext;
import org.apache.geaflow.cluster.worker.IWorker;
import org.apache.geaflow.cluster.worker.IWorkerContext;
import org.apache.geaflow.common.metric.EventMetrics;
import org.apache.geaflow.runtime.core.worker.context.AbstractWorkerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractExecutableCommand implements IExecutableCommand {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractExecutableCommand.class);

    /**
     * Scheduler id of the current event.
     */
    protected long schedulerId;

    protected int workerId;

    /**
     * Cycle id of the current event.
     */
    protected int cycleId;

    /**
     * window id of cycle.
     */
    protected long windowId;

    protected transient IWorker worker;
    protected transient IWorkerContext context;
    protected transient FetcherRunner fetcherRunner;
    protected transient EmitterRunner emitterRunner;

    public AbstractExecutableCommand(long schedulerId, int workerId, int cycleId, long windowId) {
        this.schedulerId = schedulerId;
        this.workerId = workerId;
        this.cycleId = cycleId;
        this.windowId = windowId;
    }


    @Override
    public void execute(ITaskContext taskContext) {
        worker = taskContext.getWorker();
        context = worker.getWorkerContext();
        int workerIndex = taskContext.getWorkerIndex();
        fetcherRunner = taskContext.getFetcherService().getRunner(workerIndex);
        emitterRunner = taskContext.getEmitterService().getRunner(workerIndex);
        LOGGER.info("task {} process {} batchId {}",
            context == null ? null : ((AbstractWorkerContext) context).getTaskId(), this, windowId);
    }

    public long getSchedulerId() {
        return this.schedulerId;
    }

    @Override
    public int getWorkerId() {
        return this.workerId;
    }

    public int getCycleId() {
        return cycleId;
    }

    public long getIterationWindowId() {
        return windowId;
    }

    @Override
    public void interrupt() {
        worker.interrupt();
    }

    /**
     * Finish compute and tell scheduler finish.
     *
     * @param cycleId
     * @param windowId
     * @param eventType
     */
    protected <T> void sendDoneEvent(String driverId, EventType sourceEventType, T result, boolean sendMetrics) {
        AbstractWorkerContext workerContext = (AbstractWorkerContext) this.context;
        int taskId = workerContext.getTaskId();
        EventMetrics eventMetrics = sendMetrics ? workerContext.getEventMetrics() : null;
        DoneEvent<T> doneEvent = new DoneEvent<>(this.schedulerId, this.cycleId, this.windowId, taskId, sourceEventType, result, eventMetrics);
        RpcClient.getInstance().processPipeline(driverId, doneEvent);
    }

}
