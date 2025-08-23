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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.geaflow.api.trait.CancellableTrait;
import org.apache.geaflow.cluster.collector.AbstractPipelineOutputCollector;
import org.apache.geaflow.cluster.protocol.EventType;
import org.apache.geaflow.cluster.protocol.InputMessage;
import org.apache.geaflow.cluster.response.IResult;
import org.apache.geaflow.cluster.rpc.RpcClient;
import org.apache.geaflow.cluster.worker.IWorker;
import org.apache.geaflow.cluster.worker.IWorkerContext;
import org.apache.geaflow.collector.ICollector;
import org.apache.geaflow.collector.IResultCollector;
import org.apache.geaflow.common.exception.GeaflowRuntimeException;
import org.apache.geaflow.common.metric.EventMetrics;
import org.apache.geaflow.core.graph.util.ExecutionTaskUtils;
import org.apache.geaflow.model.record.BatchRecord;
import org.apache.geaflow.model.record.RecordArgs;
import org.apache.geaflow.runtime.core.protocol.DoneEvent;
import org.apache.geaflow.runtime.core.worker.context.AbstractWorkerContext;
import org.apache.geaflow.shuffle.message.PipelineMessage;
import org.apache.geaflow.shuffle.serialize.IMessageIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractWorker<T, O> implements IWorker<BatchRecord<T>, O> {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractWorker.class);

    private static final int DEFAULT_TIMEOUT_MS = 100;

    protected AbstractWorkerContext context;
    protected InputReader<T> inputReader;
    protected Map<Long, Long> windowCount;
    protected volatile boolean running;

    public AbstractWorker() {
        this.inputReader = new InputReader<>();
        this.windowCount = new HashMap<>();
        this.running = false;
    }

    @Override
    public void open(IWorkerContext workerContext) {
        this.context = (AbstractWorkerContext) workerContext;
        this.running = true;
    }

    @Override
    public IWorkerContext getWorkerContext() {
        return context;
    }

    public InputReader<T> getInputReader() {
        return inputReader;
    }

    /**
     * Fetch message from input queue and trigger aligned compute all the time,
     * and finish until total batch count has fetched.
     */
    public void process(long totalWindowCount, boolean isAligned) {
        long processedWindowCount = 0;
        long fetchCost = 0;
        while (processedWindowCount < totalWindowCount && running) {
            try {
                long fetchStart = System.currentTimeMillis();
                InputMessage<T> input = this.inputReader.poll(DEFAULT_TIMEOUT_MS, TimeUnit.MILLISECONDS);
                fetchCost += System.currentTimeMillis() - fetchStart;
                if (input != null) {
                    long windowId = input.getWindowId();
                    if (input.getMessage() != null) {
                        PipelineMessage<T> message = input.getMessage();
                        processMessage(windowId, message);
                    } else {
                        this.context.getEventMetrics().addShuffleReadCostMs(fetchCost);
                        fetchCost = 0;
                        long totalCount = input.getWindowCount();
                        processBarrier(windowId, totalCount);
                        processedWindowCount++;
                    }
                }
            } catch (Throwable t) {
                LOGGER.error(t.getMessage(), t);
                throw new GeaflowRuntimeException(t);
            }
        }
        if (!running) {
            LOGGER.info("{} worker terminated", context.getTaskId());
        }
    }

    /**
     * Process message event and trigger worker to process.
     */
    protected void processMessageEvent(long windowId, PipelineMessage<T> message) {
        IMessageIterator<T> messageIterator = message.getMessageIterator();
        process(new BatchRecord<>(message.getRecordArgs(), messageIterator));

        long count = messageIterator.getSize();
        messageIterator.close();

        // Aggregate message not take into account when check message count.
        if (!message.getRecordArgs().getName().equals(RecordArgs.GraphRecordNames.Aggregate.name())) {
            if (!windowCount.containsKey(windowId)) {
                windowCount.put(windowId, count);
            } else {
                long oldCounter = windowCount.get(windowId);
                windowCount.put(windowId, oldCounter + count);
            }
        }
    }

    /**
     * Tell scheduler finish and send back response to scheduler.
     */
    protected void finishWindow(long windowId) {
        Map<Integer, IResult<?>> results = new HashMap<>();
        List<ICollector<?>> collectors = this.context.getCollectors();
        if (ExecutionTaskUtils.isCycleTail(context.getExecutionTask())) {
            for (int i = 0; i < collectors.size(); i++) {
                IResultCollector<?> responseCollector = (IResultCollector<?>) collectors.get(i);
                IResult<?> result = (IResult<?>) responseCollector.collectResult();
                if (result != null) {
                    results.put(result.getId(), result);
                }
            }

            // Tell scheduler finish or response.
            EventMetrics eventMetrics = this.context.isIterativeTask() ? this.context.getEventMetrics() : null;
            DoneEvent<?> done = new DoneEvent<>(context.getSchedulerId(), context.getCycleId(), windowId,
                context.getTaskId(), EventType.EXECUTE_COMPUTE, results, eventMetrics);
            RpcClient.getInstance().processPipeline(context.getDriverId(), done);
        }
    }


    protected void updateWindowId(long windowId) {
        context.setCurrentWindowId(windowId);
        for (ICollector<?> collector : this.context.getCollectors()) {
            if (collector instanceof AbstractPipelineOutputCollector) {
                ((AbstractPipelineOutputCollector<?>) collector).setWindowId(windowId);
            }
        }
    }

    protected abstract void processMessage(long windowId, PipelineMessage<T> message);

    protected abstract void processBarrier(long windowId, long totalCount);

    @Override
    public void interrupt() {
        this.running = false;
        if (context.getProcessor() instanceof CancellableTrait) {
            ((CancellableTrait) context.getProcessor()).cancel();
        }
    }

    @Override
    public void close() {
        if (context != null) {
            context.close();
        }
    }
}
