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

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import org.apache.geaflow.cluster.exception.ComponentUncaughtExceptionHandler;
import org.apache.geaflow.cluster.protocol.InputMessage;
import org.apache.geaflow.common.exception.GeaflowRuntimeException;
import org.apache.geaflow.common.thread.Executors;
import org.apache.geaflow.common.utils.ExecutorUtil;
import org.apache.geaflow.shuffle.message.PipelineMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractUnAlignedWorker<T, R> extends AbstractComputeWorker<T, R> {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractUnAlignedWorker.class);

    private static final String WORKER_FORMAT = "geaflow-asp-worker-";
    private static final int DEFAULT_TIMEOUT_MS = 100;

    protected ExecutorService executorService;
    protected BlockingQueue<Long> processingWindowIdQueue;

    public AbstractUnAlignedWorker() {
        super();
        this.processingWindowIdQueue = new LinkedBlockingDeque<>();
    }

    @Override
    public void process(long fetchCount, boolean isAligned) {
        // TODO Currently LoadGraphProcessEvent need align processing.
        if (isAligned) {
            alignedProcess(fetchCount);
        } else {
            if (executorService == null) {
                LOGGER.info("taskId {} unaligned worker has been shutdown, start...", context.getTaskId());
                startTask();
            }
        }
    }

    private void startTask() {
        long start = System.currentTimeMillis();
        this.executorService = Executors.getExecutorService(1, WORKER_FORMAT + context.getTaskId() + "-%d",
            ComponentUncaughtExceptionHandler.INSTANCE);
        executorService.execute(new WorkerTask());
        LOGGER.info("taskId {} start task cost {}ms", context != null ? context.getTaskId() : "null", System.currentTimeMillis() - start);
    }

    public void alignedProcess(long fetchCount) {
        super.process(fetchCount, true);
    }

    public void unalignedProcess() {
        try {
            InputMessage input = inputReader.poll(DEFAULT_TIMEOUT_MS, TimeUnit.MILLISECONDS);
            if (input != null) {
                long windowId = input.getWindowId();
                if (input.getMessage() != null) {
                    PipelineMessage message = input.getMessage();
                    processMessage(windowId, message);
                } else {
                    long totalCount = input.getWindowCount();
                    processBarrier(windowId, totalCount);
                }
            }
        } catch (Throwable t) {
            if (running) {
                LOGGER.error(t.getMessage(), t);
                throw new GeaflowRuntimeException(t);
            } else {
                LOGGER.warn("service closed {}", t.getMessage());
            }
        }

        if (!running) {
            LOGGER.info("{} worker terminated",
                context == null ? "null" : context.getTaskId());
        }
    }

    /**
     * Process message event and trigger worker to process.
     */
    @Override
    protected void processMessage(long windowId, PipelineMessage message) {
        processMessageEvent(windowId, message);
    }

    /**
     * Trigger worker to call processor finish.
     */
    @Override
    protected void processBarrier(long windowId, long totalCount) {
        long processedCount = windowCount.containsKey(windowId) ? windowCount.get(windowId) : 0;
        finishBarrier(totalCount, processedCount);
        LOGGER.info("taskId {} windowId {} process total {} messages", context.getTaskId(), windowId, processedCount);
        windowCount.remove(windowId);
    }

    /**
     * Verify the processed count and total count, and whether the window id
     * currently processed is consistent with the window id in the context.
     */
    protected void finishBarrier(long totalCount, long processedCount) {
        if (totalCount != processedCount) {
            LOGGER.error("taskId {} {} mismatch, TotalCount:{} != ProcessCount:{}",
                context.getTaskId(), totalCount, totalCount, processedCount);
        }
        context.getEventMetrics().addShuffleReadRecords(totalCount);

        long currentWindowId;
        try {
            currentWindowId = processingWindowIdQueue.poll(DEFAULT_TIMEOUT_MS, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            throw new GeaflowRuntimeException(e);
        }
        finish(currentWindowId);

        // Current window id must be in [context.getCurrentWindowId() - 1, context.getCurrentWindowId()].
        if (currentWindowId != context.getCurrentWindowId() && currentWindowId != context.getCurrentWindowId() - 1) {
            String errorMessage = String.format("currentWindowId is %d from queue, id is %d from context",
                currentWindowId, context.getCurrentWindowId());
            LOGGER.error(errorMessage);
            throw new GeaflowRuntimeException(errorMessage);
        }
        super.init(currentWindowId + 1);
    }

    @Override
    public void interrupt() {
        super.interrupt();
        if (executorService != null) {
            ExecutorUtil.shutdown(executorService);
            executorService = null;
        }
    }

    @Override
    public void close() {
        super.close();
        this.running = false;
        this.processingWindowIdQueue.clear();
        this.windowCount.clear();
        if (executorService != null) {
            LOGGER.info("shutdown unaligned worker");
            ExecutorUtil.shutdown(executorService);
            executorService = null;
        }
    }

    public class WorkerTask implements Runnable {

        @Override
        public void run() {
            try {
                while (running) {
                    unalignedProcess();
                }
            } catch (Exception e) {
                LOGGER.error("Unaligned process encounter exception ", e);
                throw new GeaflowRuntimeException(e);
            }
        }
    }

}
