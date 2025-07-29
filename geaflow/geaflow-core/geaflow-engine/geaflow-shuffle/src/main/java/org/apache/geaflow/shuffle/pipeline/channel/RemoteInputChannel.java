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

package org.apache.geaflow.shuffle.pipeline.channel;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.geaflow.shuffle.config.ShuffleConfig;
import org.apache.geaflow.shuffle.message.SliceId;
import org.apache.geaflow.shuffle.network.ConnectionId;
import org.apache.geaflow.shuffle.network.IConnectionManager;
import org.apache.geaflow.shuffle.network.netty.ConnectionManager;
import org.apache.geaflow.shuffle.network.netty.SliceRequestClient;
import org.apache.geaflow.shuffle.pipeline.buffer.PipeBuffer;
import org.apache.geaflow.shuffle.pipeline.buffer.PipeChannelBuffer;
import org.apache.geaflow.shuffle.pipeline.fetcher.OneShardFetcher;
import org.apache.geaflow.shuffle.util.SliceNotFoundException;

/**
 * This class references the implementation of Flink's RemoteInputChannel.
 */
public class RemoteInputChannel extends AbstractInputChannel {

    // ID to distinguish this channel from other channels sharing the same TCP connection.
    private final ChannelId id = new ChannelId();

    // The connection to use to request the remote slice.
    private final ConnectionId connectionId;

    // The connection manager to use connect to the remote slice provider.
    private final ConnectionManager connectionManager;

    // The received buffers. Received buffers are enqueued by the network I/O thread and the queue.
    // is consumed by the receiving task thread.
    private final ArrayDeque<PipeBuffer> receivedBuffers = new ArrayDeque<>();

    // Flag indicating whether this channel has been released.
    private final AtomicBoolean isReleased = new AtomicBoolean();

    // Client to establish a (possibly shared) TCP connection and request the slice.
    private volatile SliceRequestClient sliceRequestClient;

    // The next expected sequence number for the next buffer. This is modified by the network
    // I/O thread only.
    private int expectedSequenceNumber = 0;

    private final boolean enableBackPressure;

    // The initial credit for this channel.
    private final int initialCredit;

    // threshold to notify available credit for this channel.
    private final int creditNotifyThreshold;

    // available credit for this channel.
    private final AtomicInteger availableCredit;


    public RemoteInputChannel(OneShardFetcher fetcher, SliceId inputSlice, int channelIndex,
                              ConnectionId connectionId, int initialBackoff, int maxBackoff,
                              long startBatchId, IConnectionManager connectionManager) {
        super(channelIndex, fetcher, inputSlice, initialBackoff, maxBackoff, startBatchId);
        this.connectionId = Preconditions.checkNotNull(connectionId);
        this.connectionManager = (ConnectionManager) connectionManager;
        ShuffleConfig config = connectionManager.getShuffleConfig();
        this.enableBackPressure = config.isBackpressureEnabled();
        // initial credit -1 means no limit.
        this.initialCredit = enableBackPressure ? config.getChannelQueueSize() : -1;
        this.availableCredit = new AtomicInteger(0);
        this.creditNotifyThreshold = initialCredit / 2;
    }

    // ------------------------------------------------------------------------
    // Consume
    // ------------------------------------------------------------------------

    @Override
    public void requestSlice(long batchId) throws IOException, InterruptedException {
        if (sliceRequestClient == null) {
            // Create a client and request the slice
            sliceRequestClient = connectionManager.createSliceRequestClient(connectionId);
            sliceRequestClient.requestSlice(inputSliceId, this, 0, initialBatchId);
        } else {
            sliceRequestClient.requestNextBatch(batchId, this);
        }
    }

    /**
     * Retriggers a remote slice request.
     */
    public void retriggerSliceRequest(SliceId sliceId) throws IOException {
        checkClientInitialized();
        checkError();

        if (increaseBackoff()) {
            sliceRequestClient.requestSlice(sliceId, this, getCurrentBackoff(), initialBatchId);
        } else {
            setError(new SliceNotFoundException(sliceId));
        }
    }

    @Override
    public Optional<PipeChannelBuffer> getNext() throws IOException {
        checkClientInitialized();
        checkError();

        final PipeBuffer next;
        final boolean moreAvailable;

        synchronized (receivedBuffers) {
            next = receivedBuffers.poll();
            moreAvailable = !receivedBuffers.isEmpty();
        }

        if (next == null) {
            if (isReleased.get()) {
                throw new IOException("Queried for a buffer after channel has been released.");
            } else {
                throw new IllegalStateException(
                    "There should always have queued buffers for unreleased channel.");
            }
        }
        if (enableBackPressure && next.isData()
            && availableCredit.incrementAndGet() >= creditNotifyThreshold) {
            notifyCreditAvailable();
        }
        return Optional.of(new PipeChannelBuffer(next, moreAvailable));
    }

    @Override
    public boolean isReleased() {
        return isReleased.get();
    }

    /**
     * Releases all exclusive and floating buffers, closes the request client.
     */
    @Override
    public void release() throws IOException {
        if (isReleased.compareAndSet(false, true)) {
            synchronized (receivedBuffers) {
                receivedBuffers.clear();
            }

            // The released flag has to be set before closing the connection to ensure that
            // buffers received concurrently with closing are properly recycled.
            if (sliceRequestClient != null) {
                sliceRequestClient.close(this);
            } else {
                connectionManager.closeOpenChannelConnections(connectionId);
            }
        }
    }

    @Override
    public String toString() {
        return "RemoteInputChannel [" + inputSliceId + " at " + connectionId + "]";
    }

    // ------------------------------------------------------------------------
    // Network I/O notifications (called by network I/O thread)
    // ------------------------------------------------------------------------

    /**
     * Gets the current number of received buffers which have not been processed yet.
     *
     * @return Buffers queued for processing.
     */
    public int getNumberOfQueuedBuffers() {
        synchronized (receivedBuffers) {
            return receivedBuffers.size();
        }
    }

    public ChannelId getInputChannelId() {
        return id;
    }

    public int getInitialCredit() {
        return initialCredit;
    }

    public int getAndResetAvailableCredit() {
        return availableCredit.getAndSet(0);
    }

    /**
     * Enqueue this input channel in the pipeline for notifying the producer of unannounced credit.
     */
    private void notifyCreditAvailable() throws IOException {
        checkClientInitialized();
        checkError();

        sliceRequestClient.notifyCreditAvailable(this);
    }

    @VisibleForTesting
    public SliceRequestClient getSliceRequestClient() {
        return sliceRequestClient;
    }

    public void onBuffer(PipeBuffer buffer, int sequenceNumber) throws IOException {
        synchronized (receivedBuffers) {
            if (isReleased.get()) {
                return;
            }

            if (expectedSequenceNumber != sequenceNumber) {
                onError(new ReorderingException(expectedSequenceNumber, sequenceNumber));
                return;
            }

            boolean wasEmpty = receivedBuffers.isEmpty();
            receivedBuffers.add(buffer);
            ++expectedSequenceNumber;

            if (wasEmpty) {
                notifyChannelNonEmpty();
            }
        }

    }

    public void onEmptyBuffer(int sequenceNumber) throws IOException {
        synchronized (receivedBuffers) {
            if (!isReleased.get()) {
                if (expectedSequenceNumber == sequenceNumber) {
                    expectedSequenceNumber++;
                } else {
                    onError(new ReorderingException(expectedSequenceNumber, sequenceNumber));
                }
            }
        }
    }

    public void onFailedFetchRequest() {
        inputFetcher.retriggerFetchRequest(this);
    }

    public void onError(Throwable cause) {
        setError(cause);
    }

    private void checkClientInitialized() {
        Preconditions.checkState(sliceRequestClient != null,
            "Bug: client is not initialized before request data.");
    }

    private static class ReorderingException extends IOException {

        private static final long serialVersionUID = -888282210356266816L;
        private final int expectedSequenceNumber;
        private final int actualSequenceNumber;

        ReorderingException(int expectedSequenceNumber, int actualSequenceNumber) {
            this.expectedSequenceNumber = expectedSequenceNumber;
            this.actualSequenceNumber = actualSequenceNumber;
        }

        @Override
        public String getMessage() {
            return String.format(
                "Buffer re-ordering: expected buffer with sequence number %d, but received %d.",
                expectedSequenceNumber, actualSequenceNumber);
        }
    }

}
