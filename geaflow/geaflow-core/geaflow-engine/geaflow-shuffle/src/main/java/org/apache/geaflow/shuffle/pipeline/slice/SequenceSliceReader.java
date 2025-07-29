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

package org.apache.geaflow.shuffle.pipeline.slice;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.geaflow.shuffle.message.SliceId;
import org.apache.geaflow.shuffle.network.netty.SliceOutputChannelHandler;
import org.apache.geaflow.shuffle.pipeline.buffer.PipeChannelBuffer;
import org.apache.geaflow.shuffle.pipeline.channel.ChannelId;
import org.apache.geaflow.shuffle.service.ShuffleManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SequenceSliceReader implements PipelineSliceListener {

    private static final Logger LOGGER = LoggerFactory.getLogger(SequenceSliceReader.class);
    private final ChannelId inputChannelId;
    private final SliceOutputChannelHandler requestHandler;

    private SliceId sliceId;
    private PipelineSliceReader sliceReader;
    private int sequenceNumber = -1;
    private int initialCredit;
    private AtomicInteger availableCredit;

    private volatile boolean isRegistered = false;
    private volatile boolean isReleased = false;

    public SequenceSliceReader(ChannelId inputChannelId, SliceOutputChannelHandler requestHandler) {
        this.inputChannelId = inputChannelId;
        this.requestHandler = requestHandler;
    }

    public void createSliceReader(SliceId sliceId, long startBatchId, int initCredit)
        throws IOException {
        this.sliceId = sliceId;
        SliceManager sliceManager = ShuffleManager.getInstance().getSliceManager();
        this.sliceReader = sliceManager.createSliceReader(sliceId, startBatchId, this);
        this.initialCredit = initCredit;
        this.availableCredit = new AtomicInteger(initCredit);
        notifyDataAvailable();
    }

    @Override
    public void notifyDataAvailable() {
        requestHandler.notifyNonEmpty(this);
    }

    public void requestBatch(long batchId) {
        sliceReader.updateRequestedBatchId(batchId);
    }

    public void addCredit(int credit) {
        int avail = this.availableCredit.addAndGet(credit);
        if (avail > initialCredit) {
            LOGGER.warn("available credit {} > initial credit {}", avail, initialCredit);
        }
    }

    public boolean hasNext() {
        return sliceReader != null && sliceReader.hasNext();
    }

    public boolean isAvailable() {
        // initial credit less than 0, means credit is unlimited.
        return sliceReader != null && sliceReader.hasNext() && (initialCredit <= 0
            || availableCredit.get() > 0);
    }

    public PipeChannelBuffer next() {
        if (isReleased) {
            throw new IllegalArgumentException("slice has been released already: " + sliceId);
        }
        PipeChannelBuffer next = sliceReader.next();
        if (next != null) {
            sequenceNumber++;
            if (next.getBuffer().isData()) {
                availableCredit.decrementAndGet();
            }
            return next;
        }
        return null;
    }

    public int getSequenceNumber() {
        return sequenceNumber;
    }

    public ChannelId getReceiverId() {
        return inputChannelId;
    }

    public void setRegistered(boolean registered) {
        this.isRegistered = registered;
    }

    public boolean isRegistered() {
        return isRegistered;
    }

    public void releaseAllResources() throws IOException {
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
        return "SequenceSliceReader{" + "sliceId=" + sliceId + '}';
    }

}
