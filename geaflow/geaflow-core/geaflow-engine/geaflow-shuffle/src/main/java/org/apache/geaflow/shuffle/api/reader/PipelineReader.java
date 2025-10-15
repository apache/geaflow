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

package org.apache.geaflow.shuffle.api.reader;

import com.google.common.base.Preconditions;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.geaflow.common.encoder.IEncoder;
import org.apache.geaflow.common.exception.GeaflowRuntimeException;
import org.apache.geaflow.common.metric.ShuffleReadMetrics;
import org.apache.geaflow.shuffle.desc.ShardInputDesc;
import org.apache.geaflow.shuffle.message.PipelineBarrier;
import org.apache.geaflow.shuffle.message.PipelineEvent;
import org.apache.geaflow.shuffle.message.PipelineMessage;
import org.apache.geaflow.shuffle.message.PipelineSliceMeta;
import org.apache.geaflow.shuffle.message.SliceId;
import org.apache.geaflow.shuffle.network.IConnectionManager;
import org.apache.geaflow.shuffle.pipeline.buffer.OutBuffer;
import org.apache.geaflow.shuffle.pipeline.buffer.PipeFetcherBuffer;
import org.apache.geaflow.shuffle.pipeline.fetcher.MultiShardFetcher;
import org.apache.geaflow.shuffle.pipeline.fetcher.OneShardFetcher;
import org.apache.geaflow.shuffle.pipeline.fetcher.ShardFetcher;
import org.apache.geaflow.shuffle.pipeline.slice.SliceManager;
import org.apache.geaflow.shuffle.pipeline.slice.SpillablePipelineSlice;
import org.apache.geaflow.shuffle.serialize.EncoderMessageIterator;
import org.apache.geaflow.shuffle.serialize.IMessageIterator;
import org.apache.geaflow.shuffle.serialize.MessageIterator;
import org.apache.geaflow.shuffle.service.ShuffleManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PipelineReader implements IShuffleReader {

    private static final Logger LOGGER = LoggerFactory.getLogger(PipelineReader.class);

    private final IConnectionManager connectionManager;
    private ReaderContext readerContext;
    private Map<Integer, IEncoder<?>> encoders;
    private String taskName;
    private ShuffleReadMetrics readMetrics;

    private int channels;
    private int totalSliceNum;
    private int processedNum;
    private long targetWindowId;

    private ShardFetcher inputFetcher;
    private volatile boolean isRunning;

    public PipelineReader(IConnectionManager connectionManager) {
        this.connectionManager = connectionManager;
    }

    @Override
    public void init(IReaderContext context) {
        this.readerContext = (ReaderContext) context;
        this.encoders = new HashMap<>();
        for (Map.Entry<Integer, ShardInputDesc> entry : this.readerContext.getInputShardMap()
            .entrySet()) {
            this.encoders.put(entry.getKey(), entry.getValue().getEncoder());
        }
        this.taskName = this.readerContext.getTaskName();
        this.channels = this.readerContext.getSliceNum();
        this.readMetrics = new ShuffleReadMetrics();
    }

    @Override
    public void fetch(long windowId) {
        Preconditions.checkArgument(windowId > 0, "window should be larger than 0");
        if (windowId <= this.targetWindowId) {
            return;
        }

        this.totalSliceNum = this.channels;
        this.targetWindowId = windowId;
        this.processedNum = 0;

        try {
            if (this.inputFetcher == null) {
                this.inputFetcher = this.createShardFetcher(this.targetWindowId);
            }
            this.inputFetcher.requestSlices(this.targetWindowId);
        } catch (IOException e) {
            LOGGER.error(e.getMessage(), e.getCause());
            throw new GeaflowRuntimeException("fetch error", e);
        }
        this.isRunning = true;
    }

    @Override
    public boolean hasNext() {
        boolean longTerm = this.targetWindowId == Long.MAX_VALUE;
        boolean moreAvailable = this.processedNum < this.totalSliceNum;
        return longTerm || moreAvailable;
    }

    @Override
    public PipelineEvent next() {
        if (!this.isRunning) {
            return null;
        }
        long startTime = System.currentTimeMillis();
        try {
            Optional<PipeFetcherBuffer> next = this.inputFetcher.getNext();
            if (next.isPresent()) {
                PipeFetcherBuffer buffer = next.get();
                if (buffer.isBarrier()) {
                    if (this.targetWindowId != Long.MAX_VALUE && (
                        buffer.getBatchId() == this.targetWindowId || buffer.isFinish())) {
                        this.processedNum++;
                    }

                    SliceId sliceId = buffer.getSliceId();
                    PipelineBarrier barrier = new PipelineBarrier(buffer.getBatchId(),
                        sliceId.getEdgeId(), sliceId.getShardIndex(), sliceId.getSliceIndex(),
                        buffer.getBatchCount());
                    barrier.setFinish(buffer.isFinish());
                    return barrier;
                } else {
                    int edgeId = buffer.getSliceId().getEdgeId();
                    this.readMetrics.increaseDecodeBytes(buffer.getBufferSize());
                    IMessageIterator<?> msgIterator = this.getMessageIterator(edgeId,
                        buffer.getBuffer());
                    return new PipelineMessage<>(edgeId, buffer.getBatchId(),
                        buffer.getStreamName(), msgIterator);
                }
            } else {
                return null;
            }
        } catch (IOException | InterruptedException e) {
            LOGGER.error(e.getMessage(), e.getCause());
            throw new GeaflowRuntimeException(e);
        } finally {
            this.readMetrics.incFetchWaitMs(System.currentTimeMillis() - startTime);
        }
    }

    @Override
    public ShuffleReadMetrics getShuffleReadMetrics() {
        return this.readMetrics;
    }

    @Override
    public void close() {
        checkIfCloseable();

        this.isRunning = false;
        if (this.inputFetcher != null) {
            this.inputFetcher.close();
        }
    }

    private void checkIfCloseable() {
        boolean longTerm = this.targetWindowId == Long.MAX_VALUE;
        boolean moreAvailable = this.processedNum < this.totalSliceNum;
        if (!longTerm && moreAvailable) {
            throw new GeaflowRuntimeException("shuffle reader has unfinished messages");
        }
    }

    private ShardFetcher createShardFetcher(long targetWindowId) {
        Map<Integer, List<PipelineSliceMeta>> inputSlices = this.readerContext.getInputSlices();

        int fetcherIndex = 0;

        List<OneShardFetcher> fetchers = new ArrayList<>(inputSlices.size());
        Map<Integer, ShardInputDesc> inputShards = this.readerContext.getInputShardMap();
        for (Map.Entry<Integer, ShardInputDesc> entry : inputShards.entrySet()) {
            Integer edgeId = entry.getKey();
            ShardInputDesc inputDesc = entry.getValue();
            String streamName = inputDesc.getName();
            List<PipelineSliceMeta> slices = inputDesc.isPrefetchRead()
                ? this.buildPrefetchSlice(inputSlices.get(edgeId))
                : inputSlices.get(edgeId);
            OneShardFetcher inputFetcher = new OneShardFetcher(
                this.readerContext.getVertexId(),
                this.taskName,
                fetcherIndex,
                edgeId,
                streamName,
                slices,
                targetWindowId,
                this.connectionManager);
            fetchers.add(inputFetcher);
            fetcherIndex++;
        }

        if (fetchers.size() == 1) {
            return fetchers.get(0);
        } else {
            return new MultiShardFetcher(fetchers.toArray(new OneShardFetcher[0]));
        }
    }

    private List<PipelineSliceMeta> buildPrefetchSlice(List<PipelineSliceMeta> slices) {
        PipelineSliceMeta slice = slices.get(0);
        SliceId tmp = slice.getSliceId();
        SliceId sliceId = new SliceId(tmp.getPipelineId(), tmp.getEdgeId(), -1,
            tmp.getSliceIndex());
        SliceManager sliceManager = ShuffleManager.getInstance().getSliceManager();
        SpillablePipelineSlice resultSlice = (SpillablePipelineSlice) sliceManager.getSlice(
            sliceId);
        if (resultSlice == null || !resultSlice.isReady2read() || resultSlice.isReleased()) {
            throw new GeaflowRuntimeException("illegal slice: " + sliceId);
        }
        PipelineSliceMeta newSlice = new PipelineSliceMeta(sliceId, slice.getWindowId(),
            this.connectionManager.getShuffleAddress());
        return Collections.singletonList(newSlice);
    }

    private IMessageIterator<?> getMessageIterator(int edgeId, OutBuffer outBuffer) {
        IEncoder<?> encoder = this.encoders.get(edgeId);
        return encoder == null
            ? new MessageIterator<>(outBuffer)
            : new EncoderMessageIterator<>(outBuffer, encoder);
    }

}
