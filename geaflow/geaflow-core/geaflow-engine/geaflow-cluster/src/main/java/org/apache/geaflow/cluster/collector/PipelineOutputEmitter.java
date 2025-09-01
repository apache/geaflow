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

package org.apache.geaflow.cluster.collector;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.geaflow.cluster.exception.ComponentUncaughtExceptionHandler;
import org.apache.geaflow.cluster.protocol.OutputMessage;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.common.encoder.IEncoder;
import org.apache.geaflow.common.exception.GeaflowRuntimeException;
import org.apache.geaflow.common.metric.EventMetrics;
import org.apache.geaflow.common.metric.ShuffleWriteMetrics;
import org.apache.geaflow.common.task.TaskArgs;
import org.apache.geaflow.common.thread.Executors;
import org.apache.geaflow.io.AbstractMessageBuffer;
import org.apache.geaflow.model.record.RecordArgs;
import org.apache.geaflow.shuffle.ForwardOutputDesc;
import org.apache.geaflow.shuffle.OutputDescriptor;
import org.apache.geaflow.shuffle.api.writer.IShuffleWriter;
import org.apache.geaflow.shuffle.api.writer.IWriterContext;
import org.apache.geaflow.shuffle.api.writer.WriterContext;
import org.apache.geaflow.shuffle.config.ShuffleConfig;
import org.apache.geaflow.shuffle.desc.IOutputDesc;
import org.apache.geaflow.shuffle.desc.OutputType;
import org.apache.geaflow.shuffle.message.Shard;
import org.apache.geaflow.shuffle.service.ShuffleManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PipelineOutputEmitter {

    private static final Logger LOGGER = LoggerFactory.getLogger(PipelineOutputEmitter.class);

    private static final ExecutorService EMIT_EXECUTOR = Executors.getUnboundedExecutorService(
        PipelineOutputEmitter.class.getSimpleName(), 60, TimeUnit.SECONDS, null, ComponentUncaughtExceptionHandler.INSTANCE);

    private static final int DEFAULT_TIMEOUT_MS = 100;

    private final Configuration configuration;
    private final int index;
    private final Map<Integer, InitEmitterRequest> initRequestCache = new HashMap<>();
    private final Map<Integer, AtomicBoolean[]> runningFlags = new HashMap<>();

    public PipelineOutputEmitter(Configuration configuration, int index) {
        this.configuration = configuration;
        this.index = index;
    }

    public void init(InitEmitterRequest request) {
        this.initRequestCache.put(request.getTaskId(), request);
        UpdateEmitterRequest updateEmitterRequest = new UpdateEmitterRequest(
            request.getTaskId(),
            request.getWindowId(),
            request.getPipelineId(),
            request.getPipelineName(),
            request.getOutputBuffers());
        this.update(updateEmitterRequest);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    public void update(UpdateEmitterRequest request) {
        int taskId = request.getTaskId();
        if (!this.initRequestCache.containsKey(taskId)) {
            throw new GeaflowRuntimeException("init emitter request not found for task " + taskId);
        }
        InitEmitterRequest initEmitterRequest = this.initRequestCache.get(taskId);
        OutputDescriptor outputDescriptor = initEmitterRequest.getOutputDescriptor();
        List<IOutputMessageBuffer<?, Shard>> outputBuffers = request.getOutputBuffers();
        List<IOutputDesc> outputDescList = outputDescriptor.getOutputDescList();

        int outputNum = outputDescList.size();
        AtomicBoolean[] flags = new AtomicBoolean[outputNum];
        ShuffleConfig shuffleConfig = ShuffleManager.getInstance().getShuffleConfig();
        for (int i = 0; i < outputNum; i++) {
            IOutputDesc outputDesc = outputDescList.get(i);
            if (outputDesc.getType() == OutputType.RESPONSE) {
                continue;
            }
            ForwardOutputDesc forwardOutputDesc = (ForwardOutputDesc) outputDesc;
            IShuffleWriter<?, Shard> pipeRecordWriter = ShuffleManager.getInstance().loadShuffleWriter();
            IEncoder<?> encoder = forwardOutputDesc.getEncoder();
            if (encoder != null) {
                encoder.init(initEmitterRequest.getConfiguration());
            }
            TaskArgs taskArgs = initEmitterRequest.getTaskArgs();
            IWriterContext writerContext = WriterContext.newBuilder()
                .setPipelineId(request.getPipelineId())
                .setPipelineName(request.getPipelineName())
                .setConfig(shuffleConfig)
                .setVertexId(forwardOutputDesc.getPartitioner().getOpId())
                .setEdgeId(forwardOutputDesc.getEdgeId())
                .setTaskId(taskArgs.getTaskId())
                .setTaskIndex(taskArgs.getTaskIndex())
                .setTaskName(taskArgs.getTaskName())
                .setChannelNum(forwardOutputDesc.getTargetTaskIndices().size())
                .setEncoder(encoder)
                .setDataExchangeMode(forwardOutputDesc.getDataExchangeMode());
            pipeRecordWriter.init(writerContext);

            AtomicBoolean flag = new AtomicBoolean(true);
            flags[i] = flag;
            String emitterId = String.format("%d[%d/%d]", taskId, taskArgs.getTaskIndex(), taskArgs.getParallelism());
            EmitterTask emitterTask = new EmitterTask(
                pipeRecordWriter,
                outputBuffers.get(i),
                flag,
                request.getWindowId(),
                this.index,
                forwardOutputDesc.getEdgeName(),
                emitterId);
            EMIT_EXECUTOR.execute(emitterTask);
        }
        this.runningFlags.put(taskId, flags);
    }

    public void close(CloseEmitterRequest request) {
        int taskId = request.getTaskId();
        this.initRequestCache.remove(taskId);
        this.handleRunningFlags(taskId);
    }

    public void stash(StashEmitterRequest request) {
        this.handleRunningFlags(request.getTaskId());
    }

    public Configuration getConfiguration() {
        return this.configuration;
    }

    public void clear() {
        LOGGER.info("clear emitter cache of task {}", this.initRequestCache.keySet());
        this.initRequestCache.clear();
    }

    private void handleRunningFlags(int taskId) {
        if (!this.runningFlags.containsKey(taskId)) {
            return;
        }
        for (AtomicBoolean flag : this.runningFlags.remove(taskId)) {
            if (flag != null) {
                flag.set(false);
            }
        }
    }

    private static class EmitterTask<T> implements Runnable {

        private static final String WRITER_NAME_PATTERN = "shuffle-writer-%d-%s";

        private final IShuffleWriter<T, Shard> writer;
        private final IOutputMessageBuffer<T, Shard> pipe;
        private final AtomicBoolean running;
        private final long windowId;
        private final String name;
        private final String emitterId;
        private final boolean isMessage;

        public EmitterTask(IShuffleWriter<T, Shard> writer,
                           IOutputMessageBuffer<T, Shard> pipe,
                           AtomicBoolean running,
                           long windowId,
                           int workerIndex,
                           String edgeName,
                           String emitterId) {
            this.writer = writer;
            this.pipe = pipe;
            this.running = running;
            this.windowId = windowId;
            this.name = String.format(WRITER_NAME_PATTERN, workerIndex, edgeName);
            this.emitterId = emitterId;
            this.isMessage = edgeName.equals(RecordArgs.GraphRecordNames.Message.name());
        }

        @Override
        public void run() {
            Thread.currentThread().setName(this.name);
            try {
                this.execute();
            } catch (Throwable t) {
                this.pipe.error(t);
                LOGGER.error("emitter task err in window id {} {}", this.windowId, this.emitterId, t);
                throw new GeaflowRuntimeException(t);
            }
            LOGGER.info("emitter task finish window id {} {}", this.windowId, this.emitterId);
        }

        private void execute() throws Exception {
            while (this.running.get()) {
                OutputMessage<T> record = this.pipe.poll(DEFAULT_TIMEOUT_MS, TimeUnit.MILLISECONDS);
                if (record == null) {
                    continue;
                }
                long windowId = record.getWindowId();
                if (record.isBarrier()) {
                    Optional<Shard> result = this.writer.flush(windowId);
                    this.handleMetrics();
                    this.pipe.setResult(windowId, result.orElse(null));
                } else {
                    this.writer.emit(windowId, record.getMessage(), false, record.getTargetChannel());
                }
            }
            this.writer.close();
        }

        @SuppressWarnings("unchecked")
        private void handleMetrics() {
            ShuffleWriteMetrics shuffleWriteMetrics = this.writer.getShuffleWriteMetrics();
            EventMetrics eventMetrics = ((AbstractMessageBuffer<T>) this.pipe).getEventMetrics();
            if (this.isMessage) {
                // When send message, all iteration share the same context and writer, just set the total metric.
                eventMetrics.setShuffleWriteRecords(shuffleWriteMetrics.getWrittenRecords());
                eventMetrics.setShuffleWriteBytes(shuffleWriteMetrics.getEncodedSize());
            } else {
                // In FINISH iteration or other case, just add output metric.
                eventMetrics.addShuffleWriteRecords(shuffleWriteMetrics.getWrittenRecords());
                eventMetrics.addShuffleWriteBytes(shuffleWriteMetrics.getEncodedSize());
            }

        }

    }

}
