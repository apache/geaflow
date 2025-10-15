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

package org.apache.geaflow.shuffle.message;

import org.apache.geaflow.common.shuffle.ShuffleAddress;

public class PipelineSliceMeta extends BaseSliceMeta {

    private final SliceId sliceId;
    private final ShuffleAddress shuffleAddress;

    public PipelineSliceMeta(int sourceIndex, int targetIndex, long pipelineId, int edgeId,
                             ShuffleAddress address) {
        super(sourceIndex, targetIndex);
        this.windowId = -1;
        this.sliceId = new SliceId(pipelineId, edgeId, sourceIndex, targetIndex);
        this.shuffleAddress = address;
        setEdgeId(edgeId);
    }

    public PipelineSliceMeta(SliceId sliceId, long windowId, ShuffleAddress address) {
        super(sliceId.getShardIndex(), sliceId.getSliceIndex());
        this.windowId = windowId;
        this.sliceId = sliceId;
        this.shuffleAddress = address;
        setEdgeId(sliceId.getEdgeId());
    }

    public SliceId getSliceId() {
        return sliceId;
    }

    public ShuffleAddress getShuffleAddress() {
        return shuffleAddress;
    }
}
