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

package org.apache.geaflow.shuffle.api.writer;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.apache.geaflow.common.shuffle.ShuffleAddress;
import org.apache.geaflow.shuffle.message.ISliceMeta;
import org.apache.geaflow.shuffle.message.PipelineSliceMeta;
import org.apache.geaflow.shuffle.message.Shard;
import org.apache.geaflow.shuffle.message.SliceId;
import org.apache.geaflow.shuffle.pipeline.slice.IPipelineSlice;
import org.apache.geaflow.shuffle.pipeline.slice.SpillablePipelineSlice;

public class SpillableShardWriter<T> extends ShardWriter<T, Shard> {

    protected final ShuffleAddress shuffleAddress;

    public SpillableShardWriter(ShuffleAddress shuffleAddress) {
        this.shuffleAddress = shuffleAddress;
    }

    @Override
    protected IPipelineSlice newSlice(String taskLogTag, SliceId sliceId) {
        return new SpillablePipelineSlice(taskLogTag, sliceId);
    }

    @Override
    public Optional<Shard> doFinish(long windowId) {
        List<ISliceMeta> slices = this.buildSliceMeta(windowId);
        return Optional.of(new Shard(this.edgeId, slices));
    }

    private List<ISliceMeta> buildSliceMeta(long windowId) {
        List<ISliceMeta> slices = new ArrayList<>();
        for (int i = 0; i < this.targetChannels; i++) {
            SliceId sliceId = this.resultSlices[i].getSliceId();
            PipelineSliceMeta sliceMeta = new PipelineSliceMeta(sliceId, windowId, this.shuffleAddress);
            sliceMeta.setRecordNum(this.recordCounter[i]);
            sliceMeta.setEncodedSize(this.bytesCounter[i]);
            slices.add(sliceMeta);
        }
        return slices;
    }

}
