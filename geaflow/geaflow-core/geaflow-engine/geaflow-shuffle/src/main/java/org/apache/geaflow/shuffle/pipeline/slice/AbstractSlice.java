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

import java.util.ArrayDeque;
import org.apache.geaflow.common.exception.GeaflowRuntimeException;
import org.apache.geaflow.shuffle.message.SliceId;
import org.apache.geaflow.shuffle.pipeline.buffer.PipeBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractSlice implements IPipelineSlice {

    private static final Logger LOGGER = LoggerFactory.getLogger(PipelineSlice.class);

    protected final SliceId sliceId;
    protected final String taskLogTag;
    protected int totalBufferCount;
    protected ArrayDeque<PipeBuffer> buffers;
    protected PipelineSliceReader sliceReader;
    protected volatile boolean isReleased;

    public AbstractSlice(String taskLogTag, SliceId sliceId) {
        this.sliceId = sliceId;
        this.taskLogTag = taskLogTag;
        this.totalBufferCount = 0;
        this.buffers = new ArrayDeque<>();
    }

    @Override
    public SliceId getSliceId() {
        return sliceId;
    }

    @Override
    public PipelineSliceReader createSliceReader(long startBatchId, PipelineSliceListener listener) {
        synchronized (buffers) {
            if (isReleased) {
                throw new GeaflowRuntimeException("slice is released:" + sliceId);
            }
            if (sliceReader != null && sliceReader.hasNext()) {
                throw new GeaflowRuntimeException("slice is already created:" + sliceId);
            }

            LOGGER.debug("creating reader for {} {} with startBatch:{}",
                taskLogTag, sliceId, startBatchId);

            sliceReader = new DisposableSliceReader(this, startBatchId, listener);
            return sliceReader;
        }
    }

    @Override
    public boolean canRelease() {
        return !hasNext();
    }

    @Override
    public boolean isReleased() {
        return isReleased;
    }

    @Override
    public void release() {
        int bufferSize;
        final PipelineSliceReader reader;

        synchronized (buffers) {
            if (isReleased) {
                return;
            }

            // Release all available buffers
            bufferSize = buffers.size();
            buffers.clear();

            reader = sliceReader;
            sliceReader = null;
            isReleased = true;
        }

        LOGGER.info("{}: released {} with bufferSize:{}", taskLogTag, sliceId, bufferSize);
        if (reader != null) {
            reader.release();
        }
    }

}
