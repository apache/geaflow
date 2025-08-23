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

import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.geaflow.shuffle.pipeline.buffer.PipeChannelBuffer;
import org.apache.geaflow.shuffle.pipeline.channel.LocalInputChannel;

/**
 * Called by {@link SequenceSliceReader} for remote consumption
 * and {@link LocalInputChannel}
 * for local consumption.
 */
public abstract class PipelineSliceReader {

    // Client request batch id.
    private volatile long requestBatchId;
    private final PipelineSliceListener listener;

    protected final IPipelineSlice slice;
    protected volatile long consumedBatchId;
    protected final AtomicBoolean released;

    public PipelineSliceReader(IPipelineSlice slice, long startBatchId,
                               PipelineSliceListener listener) {
        this.slice = slice;
        this.listener = listener;
        this.consumedBatchId = -1;
        this.requestBatchId = startBatchId;
        this.released = new AtomicBoolean();
    }

    public void updateRequestedBatchId(long batchId) {
        this.requestBatchId = batchId;
    }

    public void notifyAvailable(long batchId) {
        if (requestBatchId == -1 || batchId <= requestBatchId) {
            listener.notifyDataAvailable();
        }
    }

    protected boolean hasBatch() {
        return requestBatchId == -1 || consumedBatchId < requestBatchId;
    }

    public abstract boolean hasNext();

    public abstract PipeChannelBuffer next();

    public void release() {
        if (released.compareAndSet(false, true) && slice.canRelease()) {
            slice.release();
        }
    }

    public boolean isReleased() {
        return released.get() || slice.isReleased();
    }

}
