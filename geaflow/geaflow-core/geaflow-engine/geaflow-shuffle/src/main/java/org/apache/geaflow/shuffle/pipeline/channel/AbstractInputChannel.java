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

import com.google.common.base.Preconditions;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.geaflow.shuffle.message.SliceId;
import org.apache.geaflow.shuffle.pipeline.buffer.PipeChannelBuffer;
import org.apache.geaflow.shuffle.pipeline.fetcher.OneShardFetcher;

/* This file is based on source code from the Flink Project (http://flink.apache.org/), licensed by the Apache
 * Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. */

/**
 * This class is an adaptation of Flink's org.apache.flink.runtime.io.network.partition.consumer.InputChannel.
 */
public abstract class AbstractInputChannel implements InputChannel {

    private static final int BACKOFF_DISABLED = -1;

    // The info of the input channel to identify it globally within a task.
    protected final int channelIndex;
    // Initial batch to fetch.
    protected final long initialBatchId;
    // Slice id to consume.
    protected final SliceId inputSliceId;
    // Parent fetcher this channel belongs to.
    protected final OneShardFetcher inputFetcher;

    // The initial backoff (ms).
    protected final int initialBackoff;
    // The maximum backoff (ms).
    protected final int maxBackoff;
    // The current backoff (ms).
    protected int currentBackoff;

    // Asynchronous error notification.
    private final AtomicReference<Throwable> cause = new AtomicReference<Throwable>();

    protected AbstractInputChannel(int channelIndex, OneShardFetcher inputFetcher, SliceId sliceId,
                                   int initialBackoff, int maxBackoff, long startBatchId) {

        Preconditions.checkArgument(channelIndex >= 0);
        Preconditions.checkArgument(initialBackoff >= 0 && initialBackoff <= maxBackoff);

        this.inputSliceId = sliceId;
        this.inputFetcher = Preconditions.checkNotNull(inputFetcher);
        this.channelIndex = channelIndex;
        this.initialBackoff = initialBackoff;
        this.maxBackoff = maxBackoff;
        this.currentBackoff = initialBackoff == 0 ? BACKOFF_DISABLED : 0;
        this.initialBatchId = startBatchId;
    }

    /**
     * Notifies the owning {@link OneShardFetcher} that this channel became non-empty.
     *
     * <p>This is guaranteed to be called only when a Buffer was added to a previously
     * empty input channel. The notion of empty is atomically consistent with the flag
     * {@link PipeChannelBuffer#moreAvailable()} when polling the next buffer
     * from this channel.
     *
     * <p><b>Note:</b> When the input channel observes an exception, this
     * method is called regardless of whether the channel was empty before. That ensures
     * that the parent InputGate will always be notified about the exception.
     */
    protected void notifyChannelNonEmpty() {
        inputFetcher.notifyChannelNonEmpty(this);
    }

    public int getChannelIndex() {
        return channelIndex;
    }

    public SliceId getInputSliceId() {
        return inputSliceId;
    }

    /**
     * Checks for an error and rethrows it if one was reported.
     */
    protected void checkError() throws IOException {
        final Throwable t = cause.get();
        if (t != null) {
            if (t instanceof IOException) {
                throw (IOException) t;
            } else {
                throw new IOException("input channel error", t);
            }
        }
    }

    /**
     * Atomically sets an error for this channel and notifies the input fetcher about available
     * data to trigger querying this channel by the task thread.
     */
    public void setError(Throwable cause) {
        if (this.cause.compareAndSet(null, Preconditions.checkNotNull(cause))) {
            // Notify the input fetcher.
            notifyChannelNonEmpty();
        }
    }

    // ------------------------------------------------------------------------
    // request exponential backoff
    // ------------------------------------------------------------------------

    /**
     * Returns the current backoff in ms.
     */
    protected int getCurrentBackoff() {
        return Math.max(currentBackoff, 0);
    }

    /**
     * Increases the current backoff and returns whether the operation was successful.
     *
     * @return <code>true</code>, iff the operation was successful. Otherwise, <code>false</code>.
     */
    protected boolean increaseBackoff() {
        // Backoff is disabled.
        if (currentBackoff < 0) {
            return false;
        }

        // This is the first time backing off
        if (currentBackoff == 0) {
            currentBackoff = initialBackoff;
            return true;
        } else if (currentBackoff < maxBackoff) {
            // Continue backing off.
            currentBackoff = Math.min(currentBackoff * 2, maxBackoff);
            return true;
        }

        return false;
    }

}
