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

package org.apache.geaflow.shuffle.pipeline.fetcher;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Timer;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.geaflow.common.exception.GeaflowRuntimeException;
import org.apache.geaflow.common.shuffle.ShuffleAddress;
import org.apache.geaflow.common.tuple.Tuple;
import org.apache.geaflow.shuffle.config.ShuffleConfig;
import org.apache.geaflow.shuffle.message.ISliceMeta;
import org.apache.geaflow.shuffle.message.PipelineSliceMeta;
import org.apache.geaflow.shuffle.message.SliceId;
import org.apache.geaflow.shuffle.network.ConnectionId;
import org.apache.geaflow.shuffle.network.IConnectionManager;
import org.apache.geaflow.shuffle.pipeline.buffer.PipeChannelBuffer;
import org.apache.geaflow.shuffle.pipeline.buffer.PipeFetcherBuffer;
import org.apache.geaflow.shuffle.pipeline.channel.AbstractInputChannel;
import org.apache.geaflow.shuffle.pipeline.channel.LocalInputChannel;
import org.apache.geaflow.shuffle.pipeline.channel.RemoteInputChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is an adaptation of Flink's org.apache.flink.runtime.io.network.partition.consumer.SingleInputGate.
 */
public class OneShardFetcher implements ShardFetcher {

    private static final Logger LOGGER = LoggerFactory.getLogger(OneShardFetcher.class);
    private static final int DEFAULT_CONNECTION_ID = -1;
    private static final String DEFAULT_STREAM_NAME = "";

    private final int stageId;
    // The name of the owning task, for logging purposes.
    private final String taskName;
    // Fetcher index starting from 0, 1, ...
    private final int fetcherIndex;
    // Lock object to guard partition requests and runtime channel updates.
    private final Object requestLock = new Object();

    // Registered listeners to forward message notifications to.
    private final List<ShardFetcherListener> fetcherListeners = new ArrayList<>();
    // Field guaranteeing uniqueness for inputChannelsWithData queue.
    private final BitSet enqueuedInputChannelsWithData;
    // Channels, which notified this input fetcher about available data.
    private final ArrayDeque<AbstractInputChannel> inputChannelsWithData = new ArrayDeque<>();
    // Next input channel to fetch.
    private AbstractInputChannel nextInputChannel;
    // Flag indicating whether all resources have been released.
    private volatile boolean isReleased;
    // Flag indicating whether the fetcher is running.
    private volatile boolean isRunning;

    // A timer to re-trigger local partition requests. Only initialized if actually needed.
    private Timer retriggerLocalRequestTimer;
    private final ExecutorService retriggerRemoteExecutor;
    private final AtomicReference<Throwable> cause = new AtomicReference<>();

    protected final int numberOfInputChannels;
    protected final Map<SliceId, AbstractInputChannel> inputChannels;
    protected final IConnectionManager connectionManager;
    protected final String inputStream;

    public OneShardFetcher(int stageId,
                           String taskName,
                           int fetcherIndex,
                           int connectionId,
                           String inputStream,
                           List<? extends ISliceMeta> inputSlices,
                           long startBatchId,
                           IConnectionManager connectionManager) {

        this.stageId = stageId;
        this.inputStream = inputStream;
        this.taskName = Preconditions.checkNotNull(taskName);
        this.fetcherIndex = fetcherIndex;
        this.numberOfInputChannels = inputSlices.size();
        this.inputChannels = new HashMap<>(numberOfInputChannels);
        this.enqueuedInputChannelsWithData = new BitSet(numberOfInputChannels);
        this.connectionManager = connectionManager;
        this.retriggerRemoteExecutor = connectionManager.getExecutor();
        this.isReleased = false;
        this.isRunning = true;

        ShuffleConfig nettyConfig = connectionManager.getShuffleConfig();
        int initialBackoff = nettyConfig.getConnectInitialBackoffMs();
        int maxBackoff = nettyConfig.getConnectMaxBackoffMs();
        buildInputChannels(connectionId, inputSlices, initialBackoff, maxBackoff, startBatchId);
    }

    @VisibleForTesting
    public OneShardFetcher(int stageId,
                           String taskName,
                           int fetcherIndex,
                           List<? extends ISliceMeta> inputSlices,
                           long startBatchId,
                           IConnectionManager connectionManager) {

        this(stageId, taskName, fetcherIndex, DEFAULT_CONNECTION_ID, DEFAULT_STREAM_NAME,
            inputSlices, startBatchId, connectionManager);
    }

    protected void buildInputChannels(int connectionId, List<? extends ISliceMeta> inputSlices,
                                      int initialBackoff, int maxBackoff, long initialBatchId) {

        int localChannels = 0;
        ShuffleAddress localAddr = connectionManager.getShuffleAddress();
        for (int inputChannelIdx = 0; inputChannelIdx < numberOfInputChannels; inputChannelIdx++) {
            PipelineSliceMeta task = (PipelineSliceMeta) inputSlices.get(inputChannelIdx);
            ShuffleAddress address = task.getShuffleAddress();
            SliceId inputSlice = task.getSliceId();

            AbstractInputChannel inputChannel;
            if (address.equals(localAddr)) {
                inputChannel = new LocalInputChannel(this, inputSlice, inputChannelIdx,
                    initialBackoff, maxBackoff, initialBatchId);
                inputChannels.put(inputSlice, inputChannel);
                localChannels++;
            } else {
                inputChannel = new RemoteInputChannel(this, inputSlice, inputChannelIdx,
                    new ConnectionId(address, connectionId), initialBackoff, maxBackoff,
                    initialBatchId, connectionManager);
                inputChannels.put(inputSlice, inputChannel);
            }
        }
        LOGGER.info("{} create {} local channels in {} channels", taskName, localChannels,
            numberOfInputChannels);
    }

    // ------------------------------------------------------------------------
    // Consume
    // ------------------------------------------------------------------------

    @Override
    public void requestSlices(long batchId) {
        synchronized (requestLock) {
            if (isReleased) {
                throw new IllegalStateException("Already released.");
            }

            // Sanity checks
            if (numberOfInputChannels != inputChannels.size()) {
                throw new IllegalStateException(String.format(
                    "Bug in input fetcher setup logic: mismatch between "
                        + "number of total input channels [%s] and the currently set number "
                        + "of input " + "channels [%s].", inputChannels.size(),
                    numberOfInputChannels));
            }

            internalRequestSlices(batchId);
        }
    }

    private void internalRequestSlices(long batchId) {
        for (AbstractInputChannel inputChannel : inputChannels.values()) {
            try {
                inputChannel.requestSlice(batchId);
            } catch (Throwable t) {
                inputChannel.setError(t);
                return;
            }
        }
        LOGGER.info("{} request next batch:{}", taskName, batchId);
    }

    public void retriggerFetchRequest(RemoteInputChannel inputChannel) {
        checkError();
        retriggerRemoteExecutor.execute(() -> {
            try {
                retriggerFetchRequest(inputChannel.getInputSliceId());
            } catch (Throwable e) {
                cause.set(e);
            }
        });
    }

    public void retriggerFetchRequest(SliceId sliceId) throws IOException {
        synchronized (requestLock) {
            if (!isReleased) {
                final AbstractInputChannel ch = inputChannels.get(sliceId);

                if (ch.getClass() == RemoteInputChannel.class) {
                    final RemoteInputChannel rch = (RemoteInputChannel) ch;
                    rch.retriggerSliceRequest(sliceId);
                } else {
                    final LocalInputChannel ich = (LocalInputChannel) ch;
                    if (retriggerLocalRequestTimer == null) {
                        retriggerLocalRequestTimer = new Timer(true);
                    }
                    ich.reTriggerSliceRequest(retriggerLocalRequestTimer);
                }
            }
        }
    }

    private void checkError() {
        final Throwable t = cause.get();
        if (t != null) {
            throw new GeaflowRuntimeException(t.getMessage(), t);
        }
    }

    @Override
    public Optional<PipeFetcherBuffer> getNext() throws IOException, InterruptedException {
        return getNext(true);
    }

    @Override
    public Optional<PipeFetcherBuffer> pollNext() throws IOException, InterruptedException {
        return getNext(false);
    }

    private Optional<PipeFetcherBuffer> getNext(boolean blocking)
        throws IOException, InterruptedException {

        if (!isRunning) {
            return Optional.empty();
        }
        if (isReleased) {
            throw new IOException("Input fetcher is already closed.");
        }
        checkError();

        Optional<InputWithData<AbstractInputChannel, PipeChannelBuffer>> next = getNextInputData(
            blocking);
        if (!next.isPresent()) {
            return Optional.empty();
        }

        InputWithData<AbstractInputChannel, PipeChannelBuffer> inputWithData = next.get();

        PipeFetcherBuffer fetcherBuffer = new PipeFetcherBuffer(inputWithData.data.getBuffer(),
            inputWithData.input.getChannelIndex(), inputWithData.moreAvailable,
            inputWithData.input.getInputSliceId(), inputStream);
        return Optional.of(fetcherBuffer);
    }

    private Optional<InputWithData<AbstractInputChannel, PipeChannelBuffer>> getNextInputData(
        boolean blocking) throws IOException, InterruptedException {

        boolean moreAvailable = false;
        AbstractInputChannel currentChannel;
        Optional<PipeChannelBuffer> result = Optional.empty();

        do {
            if (nextInputChannel != null) {
                currentChannel = nextInputChannel;
                synchronized (inputChannelsWithData) {
                    moreAvailable = inputChannelsWithData.size() > 0;
                }
            } else {
                Optional<Tuple<AbstractInputChannel, Boolean>> inputChannel = getChannel(blocking);
                if (!inputChannel.isPresent()) {
                    return Optional.empty();
                }

                currentChannel = inputChannel.get().f0;
                if (currentChannel.isReleased()) {
                    continue;
                }
                moreAvailable = inputChannel.get().f1;
            }

            result = currentChannel.getNext();

        } while (!result.isPresent());

        // this channel was now removed from the non-empty channels queue
        // we re-add it in case it has more data, because in that case no "non-empty" notification
        // will come for that channel
        if (result.get().moreAvailable()) {
            moreAvailable = true;
            if (result.get().getBuffer().isData()) {
                nextInputChannel = currentChannel;
            } else {
                queueChannel(currentChannel);
                nextInputChannel = null;
            }
        } else {
            nextInputChannel = null;
        }

        return Optional.of(new InputWithData<>(currentChannel, result.get(), moreAvailable));
    }

    // ------------------------------------------------------------------------
    // Channel notifications
    // ------------------------------------------------------------------------

    @Override
    public void registerListener(ShardFetcherListener inputFetcherListener) {
        synchronized (fetcherListeners) {
            fetcherListeners.add(inputFetcherListener);
        }
    }

    public void notifyChannelNonEmpty(AbstractInputChannel channel) {
        queueChannel(Preconditions.checkNotNull(channel));
    }

    private void queueChannel(AbstractInputChannel channel) {
        int availableChannels;

        synchronized (inputChannelsWithData) {
            if (enqueuedInputChannelsWithData.get(channel.getChannelIndex())) {
                return;
            }
            availableChannels = inputChannelsWithData.size();

            inputChannelsWithData.add(channel);
            enqueuedInputChannelsWithData.set(channel.getChannelIndex());

            if (availableChannels == 0) {
                inputChannelsWithData.notifyAll();
            }
        }

        if (availableChannels == 0) {
            synchronized (fetcherListeners) {
                for (ShardFetcherListener listener : fetcherListeners) {
                    listener.notifyAvailable(this);
                }
            }
        }

    }

    private Optional<Tuple<AbstractInputChannel, Boolean>> getChannel(boolean blocking)
        throws InterruptedException {
        if (nextInputChannel != null) {
            return Optional.of(Tuple.of(nextInputChannel, true));
        }

        synchronized (inputChannelsWithData) {
            while (inputChannelsWithData.size() == 0) {
                if (!isRunning) {
                    return Optional.empty();
                }
                if (isReleased) {
                    throw new IllegalStateException("Channel released");
                }

                if (blocking) {
                    inputChannelsWithData.wait();
                } else {
                    return Optional.empty();
                }
            }

            AbstractInputChannel inputChannel = inputChannelsWithData.remove();
            enqueuedInputChannelsWithData.clear(inputChannel.getChannelIndex());
            int availableChannels = inputChannelsWithData.size();

            return Optional.of(Tuple.of(inputChannel, availableChannels > 0));
        }
    }

    // ------------------------------------------------------------------------
    // Properties
    // ------------------------------------------------------------------------

    @Override
    public int getNumberOfInputChannels() {
        return numberOfInputChannels;
    }

    public Map<SliceId, AbstractInputChannel> getInputChannels() {
        return inputChannels;
    }

    public int getFetcherIndex() {
        return fetcherIndex;
    }

    public int getStageId() {
        return stageId;
    }

    @Override
    public boolean isFinished() {
        synchronized (requestLock) {
            for (AbstractInputChannel inputChannel : inputChannels.values()) {
                if (!inputChannel.isReleased()) {
                    return false;
                }
            }
        }

        return true;
    }

    @Override
    public void close() {
        boolean released = false;
        isRunning = false;
        synchronized (requestLock) {
            if (!isReleased) {
                try {
                    LOGGER.debug("{}: Releasing {}.", taskName, this);

                    if (this.retriggerLocalRequestTimer != null) {
                        this.retriggerLocalRequestTimer.cancel();
                        this.retriggerLocalRequestTimer = null;
                    }

                    for (AbstractInputChannel inputChannel : inputChannels.values()) {
                        try {
                            inputChannel.release();
                        } catch (IOException e) {
                            LOGGER.error("{}: Error during release of channel resources: {}.",
                                taskName, e.getMessage(), e);
                            throw new GeaflowRuntimeException(e);
                        }
                    }
                } finally {
                    released = true;
                    isReleased = true;
                }
            }
        }

        if (released) {
            synchronized (inputChannelsWithData) {
                inputChannelsWithData.notifyAll();
            }
        }
    }

}
