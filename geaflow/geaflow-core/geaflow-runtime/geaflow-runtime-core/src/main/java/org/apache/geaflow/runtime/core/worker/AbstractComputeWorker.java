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

package org.apache.geaflow.runtime.core.worker;

import com.google.common.base.Preconditions;
import org.apache.geaflow.api.trait.TransactionTrait;
import org.apache.geaflow.cluster.worker.IAffinityWorker;
import org.apache.geaflow.cluster.worker.IWorkerContext;
import org.apache.geaflow.collector.ICollector;
import org.apache.geaflow.model.record.BatchRecord;
import org.apache.geaflow.runtime.core.context.DefaultRuntimeContext;
import org.apache.geaflow.runtime.core.worker.context.AbstractWorkerContext;
import org.apache.geaflow.runtime.core.worker.context.WorkerContext;
import org.apache.geaflow.runtime.core.worker.context.WorkerContextManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractComputeWorker<T, R> extends AbstractWorker<T, R> implements TransactionTrait, IAffinityWorker {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractComputeWorker.class);

    private boolean isTransactionProcessor;

    public AbstractComputeWorker() {
        super();
    }

    @Override
    public void open(IWorkerContext workerContext) {
        super.open(workerContext);
        LOGGER.info("open processor");
        context.getProcessor().open(
            context.getCollectors(),
            context.getRuntimeContext()
        );
        this.isTransactionProcessor = context.getProcessor() instanceof TransactionTrait;
    }

    @Override
    public void init(long windowId) {
        LOGGER.info("taskId {} init windowId {}", context.getTaskId(), windowId);
        updateWindowId(windowId);
        context.getProcessor().init(windowId);
    }

    @Override
    public R process(BatchRecord<T> batchRecord) {
        return (R) context.getProcessor().process(batchRecord);
    }

    @Override
    public void finish(long windowId) {
        LOGGER.info("taskId {} finishes windowId {}, currentBatchId {}",
            context.getTaskId(), windowId, context.getCurrentWindowId());
        context.getProcessor().finish(windowId);
        finishWindow(context.getCurrentWindowId());
    }

    @Override
    public void rollback(long windowId) {
        LOGGER.info("taskId {} rollback windowId {}", context.getTaskId(), windowId);
        if (isTransactionProcessor) {
            ((TransactionTrait) context.getProcessor()).rollback(windowId);
        }
        updateWindowId(windowId + 1);
    }

    @Override
    public void stash() {
        // Stash current worker context.
        WorkerContextManager.register(context.getTaskId(), (WorkerContext) context);
        context = null;
    }

    @Override
    public void pop(IWorkerContext workerContext) {
        AbstractWorkerContext popWorkerContext = (AbstractWorkerContext) workerContext;
        context = (AbstractWorkerContext) WorkerContextManager.get(popWorkerContext.getTaskId());
        Preconditions.checkArgument(context != null, "not found any context");

        final long pipelineId = popWorkerContext.getPipelineId();
        final String pipelineName = popWorkerContext.getPipelineName();
        final int cycleId = popWorkerContext.getCycleId();
        final long windowId = popWorkerContext.getWindowId();
        final long schedulerId = popWorkerContext.getSchedulerId();

        context.setPipelineId(pipelineId);
        context.setPipelineName(pipelineName);
        context.setWindowId(windowId);
        context.setSchedulerId(schedulerId);
        context.getExecutionTask().buildTaskName(pipelineName, cycleId, windowId);
        context.initEventMetrics();

        // Update runtime context.
        DefaultRuntimeContext runtimeContext = (DefaultRuntimeContext) context.getRuntimeContext();
        runtimeContext.setPipelineId(pipelineId);
        runtimeContext.setPipelineName(pipelineName);
        runtimeContext.setWindowId(windowId);
        runtimeContext.setTaskArgs(context.getExecutionTask().buildTaskArgs());

        // Update collectors.
        for (ICollector collector : context.getCollectors()) {
            LOGGER.info("setup collector {}", runtimeContext.getTaskArgs());
            collector.setUp(runtimeContext);
        }
    }
}
