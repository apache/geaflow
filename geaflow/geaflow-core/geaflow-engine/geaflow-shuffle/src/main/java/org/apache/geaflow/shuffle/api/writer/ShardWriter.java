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

package org.apache.geaflow.shuffle.api.writer;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import org.apache.geaflow.common.encoder.IEncoder;
import org.apache.geaflow.common.metric.ShuffleWriteMetrics;
import org.apache.geaflow.shuffle.config.ShuffleConfig;
import org.apache.geaflow.shuffle.message.PipelineBarrier;
import org.apache.geaflow.shuffle.message.SliceId;
import org.apache.geaflow.shuffle.message.WriterId;
import org.apache.geaflow.shuffle.pipeline.buffer.HeapBuffer.HeapBufferBuilder;
import org.apache.geaflow.shuffle.pipeline.buffer.MemoryViewBuffer.MemoryViewBufferBuilder;
import org.apache.geaflow.shuffle.pipeline.buffer.OutBuffer.BufferBuilder;
import org.apache.geaflow.shuffle.pipeline.buffer.PipeBuffer;
import org.apache.geaflow.shuffle.pipeline.slice.IPipelineSlice;
import org.apache.geaflow.shuffle.pipeline.slice.SliceManager;
import org.apache.geaflow.shuffle.serialize.EncoderRecordSerializer;
import org.apache.geaflow.shuffle.serialize.IRecordSerializer;
import org.apache.geaflow.shuffle.serialize.RecordSerializer;
import org.apache.geaflow.shuffle.service.ShuffleManager;

public abstract class ShardWriter<T, R> {

    protected IWriterContext writerContext;
    protected ShuffleConfig shuffleConfig;
    protected ShuffleWriteMetrics writeMetrics;


    protected long pipelineId;
    protected String pipelineName;
    protected int edgeId;
    protected int taskIndex;
    protected int targetChannels;
    protected boolean enableBackPressure;

    protected String taskLogTag;
    protected long[] recordCounter;
    protected long[] bytesCounter;
    protected long maxBufferSize;

    protected BufferBuilder[] buffers;
    protected volatile IPipelineSlice[] resultSlices;
    protected IRecordSerializer<T> recordSerializer;

    //////////////////////////////
    // Init.

    /// ///////////////////////////

    public void init(IWriterContext writerContext) {
        this.writerContext = writerContext;
        this.shuffleConfig = writerContext.getConfig();
        this.writeMetrics = new ShuffleWriteMetrics();

        this.pipelineId = writerContext.getPipelineInfo().getPipelineId();
        this.pipelineName = writerContext.getPipelineInfo().getPipelineName();
        this.edgeId = writerContext.getEdgeId();
        this.taskIndex = writerContext.getTaskIndex();
        this.targetChannels = writerContext.getTargetChannelNum();
        this.taskLogTag = writerContext.getTaskName();
        this.recordCounter = new long[this.targetChannels];
        this.bytesCounter = new long[this.targetChannels];
        this.maxBufferSize = this.shuffleConfig.getMaxBufferSizeBytes();
        this.enableBackPressure = this.shuffleConfig.isBackpressureEnabled();

        this.buffers = this.buildBufferBuilder(this.targetChannels,
            this.shuffleConfig.isMemoryPoolEnable());
        this.resultSlices = this.buildResultSlices(this.targetChannels);
        this.recordSerializer = this.getRecordSerializer();
    }

    private BufferBuilder[] buildBufferBuilder(int channels, boolean enableMemoryPool) {
        BufferBuilder[] buffers = new BufferBuilder[channels];
        for (int i = 0; i < channels; i++) {
            BufferBuilder bufferBuilder = enableMemoryPool
                ? new MemoryViewBufferBuilder()
                : new HeapBufferBuilder();
            bufferBuilder.enableMemoryTrack();
            buffers[i] = bufferBuilder;
        }
        return buffers;
    }

    protected IPipelineSlice[] buildResultSlices(int channels) {
        IPipelineSlice[] slices = new IPipelineSlice[channels];
        WriterId writerId = new WriterId(this.pipelineId, this.edgeId, this.taskIndex);
        SliceManager sliceManager = ShuffleManager.getInstance().getSliceManager();
        for (int i = 0; i < channels; i++) {
            SliceId sliceId = new SliceId(writerId, i);
            IPipelineSlice slice = this.newSlice(this.taskLogTag, sliceId);
            slices[i] = slice;
            sliceManager.register(sliceId, slice);
        }
        return slices;
    }

    protected abstract IPipelineSlice newSlice(String taskLogTag, SliceId sliceId);

    @SuppressWarnings("unchecked")
    private IRecordSerializer<T> getRecordSerializer() {
        IEncoder<?> encoder = this.writerContext.getEncoder();
        if (encoder == null) {
            return new RecordSerializer<>();
        }
        return new EncoderRecordSerializer<>((IEncoder<T>) encoder);
    }

    //////////////////////////////
    // Write data.

    /// ///////////////////////////

    public void emit(long windowId, T value, boolean isRetract, int[] channels) throws IOException {
        for (int channel : channels) {
            BufferBuilder outBuffer = this.buffers[channel];
            this.recordSerializer.serialize(value, isRetract, outBuffer);
            if (outBuffer.getBufferSize() >= this.maxBufferSize) {
                this.sendBuffer(channel, outBuffer, windowId);
            }
        }
    }

    public void emit(long windowId, List<T> data, int channel) throws IOException {
        BufferBuilder outBuffer = this.buffers[channel];
        for (T datum : data) {
            this.recordSerializer.serialize(datum, false, outBuffer);
        }
        if (outBuffer.getBufferSize() >= this.maxBufferSize) {
            this.sendBuffer(channel, outBuffer, windowId);
        }
    }

    public Optional<R> finish(long windowId) throws IOException {
        this.flushFloatingBuffers(windowId);
        this.notify(new PipelineBarrier(windowId, this.edgeId, this.taskIndex));
        this.flushSlices();
        return this.doFinish(windowId);
    }

    protected abstract Optional<R> doFinish(long windowId) throws IOException;

    protected void sendBuffer(int sliceIndex, BufferBuilder builder, long windowId) {
        this.recordCounter[sliceIndex] += builder.getRecordCount();
        this.bytesCounter[sliceIndex] += builder.getBufferSize();
        IPipelineSlice resultSlice = this.resultSlices[sliceIndex];
        resultSlice.add(new PipeBuffer(builder.build(), windowId));
    }

    private void sendBarrier(int sliceIndex, long windowId, int count, boolean isFinish) {
        IPipelineSlice resultSlice = this.resultSlices[sliceIndex];
        resultSlice.add(new PipeBuffer(windowId, count, isFinish));
    }

    private void flushFloatingBuffers(long windowId) {
        for (int i = 0; i < this.targetChannels; i++) {
            BufferBuilder bufferBuilder = this.buffers[i];
            if (bufferBuilder.getBufferSize() > 0) {
                this.sendBuffer(i, bufferBuilder, windowId);
            }
        }
    }

    protected boolean flushSlices() {
        IPipelineSlice[] pipeSlices = this.resultSlices;
        boolean flushed = false;
        if (pipeSlices != null) {
            for (int i = 0; i < pipeSlices.length; i++) {
                if (null != pipeSlices[i]) {
                    pipeSlices[i].flush();
                    flushed = true;
                }
            }
        }
        return flushed;
    }

    public ShuffleWriteMetrics getShuffleWriteMetrics() {
        return this.writeMetrics;
    }

    protected void notify(PipelineBarrier barrier) throws IOException {
        for (int channel = 0; channel < this.targetChannels; channel++) {
            long windowId = barrier.getWindowId();
            long recordCount = this.recordCounter[channel];
            long bytesCount = this.bytesCounter[channel];
            sendBarrier(channel, windowId, (int) recordCount, barrier.isFinish());

            this.writeMetrics.increaseRecords(recordCount);
            this.writeMetrics.increaseEncodedSize(bytesCount);
            this.recordCounter[channel] = 0;
            this.bytesCounter[channel] = 0;
        }
    }

    public void close() {
    }

}