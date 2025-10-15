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

package org.apache.geaflow.shuffle.pipeline.buffer;

import org.apache.geaflow.shuffle.message.SliceId;
import org.apache.geaflow.shuffle.pipeline.channel.InputChannel;
import org.apache.geaflow.shuffle.pipeline.fetcher.ShardFetcher;

/**
 * Message consumed by {@link ShardFetcher}.
 * One fetcher consumes buffers from multiple {@link InputChannel}
 * which is identified by channelIndex.
 */
public class PipeFetcherBuffer {

    private final PipeBuffer buffer;
    private final SliceId sliceId;
    private final String streamName;

    // indicate if has more in the channel.
    private boolean moreAvailable;
    // current fetched channel.
    private int channelIndex;

    public PipeFetcherBuffer(PipeBuffer pipeBuffer, int channelIndex,
                             boolean moreAvailable, SliceId sliceId, String streamName) {
        this.buffer = pipeBuffer;
        this.channelIndex = channelIndex;
        this.moreAvailable = moreAvailable;
        this.sliceId = sliceId;
        this.streamName = streamName;
    }

    public boolean moreAvailable() {
        return moreAvailable;
    }

    public void setMoreAvailable(boolean moreAvailable) {
        this.moreAvailable = moreAvailable;
    }

    public int getChannelIndex() {
        return channelIndex;
    }

    public void setChannelIndex(int channelIndex) {
        this.channelIndex = channelIndex;
    }

    public SliceId getSliceId() {
        return sliceId;
    }

    public String getStreamName() {
        return streamName;
    }

    public int getBufferSize() {
        if (buffer != null) {
            return buffer.getBuffer().getBufferSize();
        } else {
            return 0;
        }
    }

    public OutBuffer getBuffer() {
        return buffer.getBuffer();
    }

    public boolean isBarrier() {
        return !buffer.isData();
    }

    public long getBatchId() {
        return buffer.getBatchId();
    }

    public int getBatchCount() {
        return buffer.getCount();
    }

    public boolean isFinish() {
        return buffer.isFinish();
    }

}
