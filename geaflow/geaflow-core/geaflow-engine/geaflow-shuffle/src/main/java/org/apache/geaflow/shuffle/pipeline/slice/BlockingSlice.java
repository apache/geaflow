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

import com.google.common.base.Preconditions;
import org.apache.geaflow.shuffle.api.writer.PipelineShardWriter;
import org.apache.geaflow.shuffle.message.SliceId;
import org.apache.geaflow.shuffle.pipeline.buffer.PipeBuffer;

public class BlockingSlice extends AbstractSlice {

    private final PipelineShardWriter parentWriter;
    private boolean flushRequested;

    public BlockingSlice(String taskLogTag, SliceId sliceId,
                         PipelineShardWriter parentWriter) {
        super(taskLogTag, sliceId);
        this.parentWriter = parentWriter;
    }

    // ------------------------------------------------------------------------
    // Produce
    // ------------------------------------------------------------------------

    @Override
    public boolean add(PipeBuffer recordBuffer) {
        final boolean notifyDataAvailable;
        synchronized (buffers) {
            if (isReleased) {
                return false;
            }
            buffers.add(recordBuffer);
            notifyDataAvailable = shouldNotifyDataAvailable();
        }

        if (notifyDataAvailable) {
            notifyDataAvailable(recordBuffer.getBatchId());
        }
        return true;
    }

    private boolean shouldNotifyDataAvailable() {
        return sliceReader != null && !this.flushRequested && getCurrentNumberOfBuffers() == 1;
    }

    @Override
    public void flush() {
        long batchId;
        boolean needNotify;
        synchronized (buffers) {
            if (buffers.isEmpty()) {
                return;
            }

            batchId = buffers.peekLast().getBatchId();
            needNotify = !flushRequested && buffers.size() == 1;
            updateFlushRequested(flushRequested || buffers.size() > 1 || needNotify);
        }

        if (needNotify) {
            notifyDataAvailable(batchId);
        }
    }

    private void notifyDataAvailable(long batchId) {
        final PipelineSliceReader reader = sliceReader;
        if (reader != null) {
            reader.notifyAvailable(batchId);
        }
    }

    // ------------------------------------------------------------------------
    // Consume
    // ------------------------------------------------------------------------

    @Override
    public boolean isReady2read() {
        return true;
    }

    @Override
    public PipeBuffer next() {
        synchronized (buffers) {
            PipeBuffer buffer = null;
            if (!buffers.isEmpty()) {
                buffer = buffers.pop();
                if (buffers.isEmpty()) {
                    updateFlushRequested(false);
                }
            }
            if (buffer != null && buffer.isData()) {
                parentWriter.notifyBufferConsumed(buffer.getBufferSize());
            }
            return buffer;
        }
    }

    @Override
    public boolean hasNext() {
        synchronized (buffers) {
            return this.flushRequested || getCurrentNumberOfBuffers() > 0;
        }
    }

    private int getCurrentNumberOfBuffers() {
        Preconditions.checkArgument(Thread.holdsLock(buffers), "fail to get lock of buffers");
        return buffers.size();
    }

    private void updateFlushRequested(boolean flushRequested) {
        Preconditions.checkArgument(Thread.holdsLock(buffers), "fail to get lock of buffers");
        this.flushRequested = flushRequested;
    }

}
