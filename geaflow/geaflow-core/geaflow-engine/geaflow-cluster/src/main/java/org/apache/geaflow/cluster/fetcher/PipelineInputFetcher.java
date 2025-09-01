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

package org.apache.geaflow.cluster.fetcher;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.geaflow.cluster.exception.ComponentUncaughtExceptionHandler;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.common.encoder.IEncoder;
import org.apache.geaflow.common.exception.GeaflowRuntimeException;
import org.apache.geaflow.common.metric.EventMetrics;
import org.apache.geaflow.common.metric.ShuffleReadMetrics;
import org.apache.geaflow.common.thread.Executors;
import org.apache.geaflow.io.AbstractMessageBuffer;
import org.apache.geaflow.shuffle.api.reader.PipelineReader;
import org.apache.geaflow.shuffle.api.reader.ReaderContext;
import org.apache.geaflow.shuffle.desc.ShardInputDesc;
import org.apache.geaflow.shuffle.message.PipelineBarrier;
import org.apache.geaflow.shuffle.message.PipelineEvent;
import org.apache.geaflow.shuffle.message.PipelineMessage;
import org.apache.geaflow.shuffle.service.ShuffleManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PipelineInputFetcher {

    private static final Logger LOGGER = LoggerFactory.getLogger(PipelineInputFetcher.class);

    private static final ExecutorService FETCH_EXECUTOR = Executors.getUnboundedExecutorService(
        PipelineInputFetcher.class.getSimpleName(), 60, TimeUnit.SECONDS, null,
        ComponentUncaughtExceptionHandler.INSTANCE);

    private final Map<Integer, FetcherTask> taskId2fetchTask = new HashMap<>();
    private final Configuration config;

    public PipelineInputFetcher(Configuration config) {
        this.config = config;
    }

    /**
     * Init input fetcher reader.
     *
     * @param request init fetch request
     */
    public void init(InitFetchRequest request) {
        int taskId = request.getTaskId();
        if (this.taskId2fetchTask.containsKey(taskId)) {
            throw new GeaflowRuntimeException("task already exists: " + taskId);
        }
        for (ShardInputDesc inputDesc : request.getInputShards().values()) {
            IEncoder<?> encoder = inputDesc.getEncoder();
            if (encoder != null) {
                encoder.init(this.config);
            }
        }
        this.taskId2fetchTask.put(taskId, new FetcherTask(this.config, request));
        LOGGER.info("init fetcher task {} {}", request.getTaskName(), request.getShufflePhases());
    }

    /**
     * Fetch data according to fetch request and process by worker.
     *
     * @param request fetch request
     */
    protected void fetch(FetchRequest request) {
        FetcherTask fetcherTask = this.taskId2fetchTask.get(request.getTaskId());
        if (fetcherTask != null) {
            long targetWindowId = request.getWindowId() + request.getWindowCount() - 1;
            fetcherTask.updateWindowId(targetWindowId);
            if (!fetcherTask.isRunning()) {
                fetcherTask.start();
                FETCH_EXECUTOR.execute(fetcherTask);
            }
        }
    }

    public void close(CloseFetchRequest request) {
        int taskId = request.getTaskId();
        FetcherTask task = this.taskId2fetchTask.remove(taskId);
        if (task != null) {
            task.close();
            LOGGER.info("close fetcher task {} {}", task.initFetchRequest.getTaskName(),
                task.initFetchRequest.getShufflePhases());
        }
    }

    public void cancel() {
        // TODO Cancel fetching task.
        //      Shuffle reader should support cancel.
    }

    /**
     * Close the shuffle reader.
     */
    public void close() {
        for (FetcherTask task : this.taskId2fetchTask.values()) {
            task.close();
        }
        this.taskId2fetchTask.clear();
    }

    private static class FetcherTask implements Runnable {

        private static final String READER_NAME_PATTERN = "shuffle-reader-%d[%d/%d]";
        private static final int WAIT_TIME_OUT_MS = 100;

        private final Configuration config;
        private final InitFetchRequest initFetchRequest;
        private final PipelineReader shuffleReader;
        private final IInputMessageBuffer<?>[] fetchListeners;
        private final BarrierHandler barrierHandler;
        private final String name;

        private volatile boolean running;
        private volatile long targetWindowId;

        private FetcherTask(Configuration config, InitFetchRequest request) {
            this.config = config;
            this.initFetchRequest = request;
            this.shuffleReader = (PipelineReader) ShuffleManager.getInstance().loadShuffleReader();
            this.shuffleReader.init(this.buildReaderContext());
            this.fetchListeners = request.getFetchListeners()
                .toArray(new IInputMessageBuffer<?>[]{});
            this.barrierHandler = new BarrierHandler(request.getTaskId(), request.getInputShards());
            this.name = String.format(READER_NAME_PATTERN, request.getTaskId(),
                request.getTaskIndex(), request.getTaskParallelism());
        }

        public void start() {
            this.running = true;
        }

        public boolean isRunning() {
            return this.running;
        }

        public void updateWindowId(long windowId) {
            if (this.targetWindowId < windowId) {
                this.targetWindowId = windowId;
                synchronized (this) {
                    this.notifyAll();
                }
            }
        }

        @Override
        public void run() {
            Thread.currentThread().setName(this.name);
            try {
                this.fetch();
            } catch (GeaflowRuntimeException e) {
                LOGGER.error("fetcher task err with window id {} {}", this.targetWindowId,
                    this.name, e);
                throw e;
            } catch (Throwable e) {
                LOGGER.error("fetcher task err with window id {} {}", this.targetWindowId,
                    this.name, e);
                throw new GeaflowRuntimeException(e.getMessage(), e);
            }
        }

        public void fetch() throws InterruptedException {
            while (this.running) {
                this.shuffleReader.fetch(this.targetWindowId);
                if (!this.shuffleReader.hasNext()) {
                    synchronized (this) {
                        this.wait(WAIT_TIME_OUT_MS);
                    }
                    continue;
                }
                PipelineEvent event = this.shuffleReader.next();
                if (event != null) {
                    if (event instanceof PipelineMessage) {
                        PipelineMessage message = (PipelineMessage) event;
                        for (IInputMessageBuffer<?> listener : this.fetchListeners) {
                            listener.onMessage(message);
                        }
                    } else {
                        PipelineBarrier barrier = (PipelineBarrier) event;
                        if (this.barrierHandler.checkCompleted(barrier)) {
                            long windowCount = this.barrierHandler.getTotalWindowCount();
                            this.handleMetrics();
                            PipelineBarrier windowBarrier = new PipelineBarrier(
                                barrier.getWindowId(), barrier.getEdgeId(), windowCount);
                            for (IInputMessageBuffer<?> listener : this.fetchListeners) {
                                listener.onBarrier(windowBarrier);
                            }
                        }
                    }
                }
            }
            LOGGER.info("fetcher task finish window id {} {}", this.targetWindowId, this.name);
        }

        private ReaderContext buildReaderContext() {
            ReaderContext context = new ReaderContext();
            context.setConfig(this.config);
            context.setVertexId(this.initFetchRequest.getVertexId());
            context.setTaskName(this.initFetchRequest.getTaskName());
            context.setInputShardMap(this.initFetchRequest.getInputShards());
            context.setInputSlices(this.initFetchRequest.getInputSlices());
            context.setSliceNum(this.initFetchRequest.getSliceNum());
            return context;
        }

        private void handleMetrics() {
            ShuffleReadMetrics shuffleReadMetrics = this.shuffleReader.getShuffleReadMetrics();
            for (IInputMessageBuffer<?> listener : this.fetchListeners) {
                if (listener instanceof AbstractMessageBuffer<?>) {
                    EventMetrics eventMetrics = ((AbstractMessageBuffer<?>) listener).getEventMetrics();
                    eventMetrics.addShuffleReadBytes(shuffleReadMetrics.getDecodeBytes());
                }
            }
        }

        public void close() {
            this.running = false;
            if (this.shuffleReader != null) {
                this.shuffleReader.close();
            }
        }

    }

}