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

package org.apache.geaflow.cluster.fetcher;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.geaflow.cluster.protocol.InputMessage;
import org.apache.geaflow.common.exception.GeaflowRuntimeException;
import org.apache.geaflow.shuffle.message.PipelineBarrier;
import org.apache.geaflow.shuffle.message.PipelineMessage;
import org.apache.geaflow.shuffle.message.SliceId;
import org.apache.geaflow.shuffle.pipeline.buffer.OutBuffer;
import org.apache.geaflow.shuffle.pipeline.buffer.PipeBuffer;
import org.apache.geaflow.shuffle.pipeline.slice.SliceManager;
import org.apache.geaflow.shuffle.pipeline.slice.SpillablePipelineSlice;
import org.apache.geaflow.shuffle.serialize.AbstractMessageIterator;
import org.apache.geaflow.shuffle.service.ShuffleManager;

public class PrefetchMessageBuffer<T> implements IInputMessageBuffer<T> {

    // The latch to notify data finish.
    private final CountDownLatch latch = new CountDownLatch(1);
    // Data slice.
    private final SpillablePipelineSlice slice;
    // Data edge id.
    private final int edgeId;

    public PrefetchMessageBuffer(String logTag, SliceId sliceId) {
        this.slice = new SpillablePipelineSlice(logTag, sliceId);
        this.edgeId = sliceId.getEdgeId();
        SliceManager sliceManager = ShuffleManager.getInstance().getSliceManager();
        sliceManager.register(sliceId, this.slice);
    }

    @Override
    public void offer(InputMessage<T> message) {
        throw new UnsupportedOperationException();
    }

    @Override
    public InputMessage<T> poll(long timeout, TimeUnit unit) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void onMessage(PipelineMessage<T> message) {
        if (message.getEdgeId() != this.edgeId) {
            return;
        }
        AbstractMessageIterator<T> iterator = (AbstractMessageIterator<T>) message.getMessageIterator();
        OutBuffer outBuffer = iterator.getOutBuffer();
        long windowId = message.getRecordArgs().getWindowId();
        this.slice.add(new PipeBuffer(outBuffer, windowId));
    }

    @Override
    public void onBarrier(PipelineBarrier barrier) {
        if (barrier.getEdgeId() != this.edgeId) {
            return;
        }
        this.slice.add(new PipeBuffer(barrier.getWindowId(), (int) barrier.getCount(), true));
        this.slice.flush();
        this.latch.countDown();
    }

    public void waitUtilFinish() {
        try {
            this.latch.await();
        } catch (InterruptedException e) {
            throw new GeaflowRuntimeException(e);
        }
    }

}
