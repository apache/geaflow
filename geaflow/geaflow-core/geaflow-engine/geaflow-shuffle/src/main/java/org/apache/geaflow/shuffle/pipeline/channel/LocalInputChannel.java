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
import java.util.Optional;
import java.util.Timer;
import java.util.TimerTask;
import org.apache.geaflow.shuffle.message.SliceId;
import org.apache.geaflow.shuffle.pipeline.buffer.PipeChannelBuffer;
import org.apache.geaflow.shuffle.pipeline.fetcher.OneShardFetcher;
import org.apache.geaflow.shuffle.pipeline.slice.PipelineSliceListener;
import org.apache.geaflow.shuffle.pipeline.slice.PipelineSliceReader;
import org.apache.geaflow.shuffle.pipeline.slice.SliceManager;
import org.apache.geaflow.shuffle.service.ShuffleManager;
import org.apache.geaflow.shuffle.util.SliceNotFoundException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/* This file is based on source code from the Flink Project (http://flink.apache.org/), licensed by the Apache
 * Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. */

/**
 * This class is an adaptation of Flink's org.apache.flink.runtime.io.network.partition.consumer.LocalInputChannel.
 */
public class LocalInputChannel extends AbstractInputChannel implements PipelineSliceListener {

    private static final Logger LOGGER = LoggerFactory.getLogger(LocalInputChannel.class);

    private final Object requestLock = new Object();
    private PipelineSliceReader sliceReader;
    private volatile boolean isReleased;

    public LocalInputChannel(
        OneShardFetcher fetcher,
        SliceId inputSlice,
        int channelIndex,
        int initialBackoff,
        int maxBackoff,
        long startBatchId) {
        super(channelIndex, fetcher, inputSlice, initialBackoff, maxBackoff, startBatchId);
    }

    @Override
    public void requestSlice(long batchId) throws IOException {
        boolean retriggerRequest = false;

        // The lock is required to request only once in the presence of retriggered requests.
        synchronized (requestLock) {
            Preconditions.checkState(!isReleased, "LocalInputChannel has been released already");
            if (this.sliceReader == null) {
                LOGGER.info("Requesting Local slice {}", this.inputSliceId);
                try {
                    SliceManager sliceManager = ShuffleManager.getInstance().getSliceManager();
                    this.sliceReader = sliceManager
                        .createSliceReader(this.inputSliceId, this.initialBatchId, this);
                } catch (SliceNotFoundException notFound) {
                    if (increaseBackoff()) {
                        retriggerRequest = true;
                    } else {
                        LOGGER.warn("not found slice:{}", this.inputSliceId);
                        throw notFound;
                    }
                }
            } else {
                this.sliceReader.updateRequestedBatchId(batchId);
            }
        }

        if (this.sliceReader != null && this.sliceReader.hasNext()) {
            notifyDataAvailable();
        }
        // Do this outside of the lock scope as this might lead to a
        // deadlock with a concurrent release of the channel via the
        // input fetcher.
        if (retriggerRequest) {
            inputFetcher.retriggerFetchRequest(inputSliceId);
        }
    }

    public void reTriggerSliceRequest(Timer timer) {
        synchronized (requestLock) {
            Preconditions.checkState(sliceReader == null, "already requested slice");
            timer.schedule(new TimerTask() {
                @Override
                public void run() {
                    try {
                        requestSlice(initialBatchId);
                    } catch (Throwable t) {
                        setError(t);
                    }
                }
            }, getCurrentBackoff());
        }
    }

    @Override
    public Optional<PipeChannelBuffer> getNext() throws IOException {
        checkError();
        PipelineSliceReader reader = this.sliceReader;
        if (reader == null) {
            if (isReleased) {
                return Optional.empty();
            }
            reader = checkAndGetSliceReader();
        }

        PipeChannelBuffer next = reader.next();

        if (next == null) {
            return Optional.empty();
        }

        return Optional.of(next);
    }

    @Override
    public void notifyDataAvailable() {
        notifyChannelNonEmpty();
    }

    private PipelineSliceReader checkAndGetSliceReader() {
        // Synchronizing on the request lock means this blocks until the asynchronous request
        // for the slice has been completed by then the slice reader is visible or the channel is released.
        synchronized (requestLock) {
            Preconditions.checkState(!isReleased, "released");
            Preconditions.checkState(sliceReader != null, "reader is not ready.");
            return sliceReader;
        }
    }

    @Override
    public boolean isReleased() {
        return isReleased;
    }

    @Override
    public void release() {
        if (!isReleased) {
            isReleased = true;
            PipelineSliceReader reader = sliceReader;
            if (reader != null) {
                reader.release();
                sliceReader = null;
            }
        }
    }

    @Override
    public String toString() {
        return "LocalInputChannel [" + inputSliceId + "]";
    }

}
