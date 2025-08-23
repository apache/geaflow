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

package org.apache.geaflow.cluster.collector;

import java.util.List;
import java.util.stream.IntStream;
import org.apache.geaflow.api.context.RuntimeContext;
import org.apache.geaflow.collector.AbstractCollector;
import org.apache.geaflow.collector.ICollector;
import org.apache.geaflow.common.exception.GeaflowRuntimeException;
import org.apache.geaflow.partitioner.IPartitioner;
import org.apache.geaflow.partitioner.impl.KeyPartitioner;
import org.apache.geaflow.selector.ISelector;
import org.apache.geaflow.selector.impl.ChannelSelector;
import org.apache.geaflow.shuffle.ForwardOutputDesc;

public abstract class AbstractPipelineCollector<T>
    extends AbstractCollector implements ICollector<T> {

    protected transient IOutputMessageBuffer<T, ?> outputBuffer;
    protected transient ISelector recordISelector;
    protected ForwardOutputDesc<T> outputDesc;
    protected long windowId;

    public AbstractPipelineCollector(ForwardOutputDesc<T> outputDesc) {
        super(outputDesc.getPartitioner().getOpId());
        this.outputDesc = outputDesc;
    }

    @Override
    public void setUp(RuntimeContext runtimeContext) {
        super.setUp(runtimeContext);
        List<Integer> targetTaskIds = outputDesc.getTargetTaskIndices();
        IPartitioner<T> partitioner = outputDesc.getPartitioner();
        if (partitioner.getPartitionType() == IPartitioner.PartitionType.key) {
            ((KeyPartitioner<T>) partitioner).init(outputDesc.getNumPartitions());
        }
        this.recordISelector = new ChannelSelector(targetTaskIds.size(),
            partitioner);
    }

    public void setOutputBuffer(IOutputMessageBuffer<T, ?> outputBuffer) {
        this.outputBuffer = outputBuffer;
    }

    public long getWindowId() {
        return windowId;
    }

    public void setWindowId(long windowId) {
        this.windowId = windowId;
    }

    @Override
    public void broadcast(T value) {
        List<Integer> targetTaskIds = outputDesc.getTargetTaskIndices();
        int[] channels = IntStream.rangeClosed(0, targetTaskIds.size() - 1).toArray();
        try {
            this.outputBuffer.emit(this.windowId, value, false, channels);
            this.outputMeter.mark();
        } catch (Exception e) {
            throw new GeaflowRuntimeException(e);
        }
    }

    @Override
    public void partition(T value) {
        shuffle(value, false);
    }

    @Override
    public <KEY> void partition(KEY key, T value) {
        shuffle(key, value, false);
    }

    @Override
    public void finish() {
        try {
            this.outputBuffer.finish(this.windowId);
        } catch (Exception e) {
            throw new GeaflowRuntimeException(e);
        }
    }

    /**
     * Shuffle data with value itself.
     */
    protected void shuffle(T value, boolean isRetract) {
        int[] targetChannels = this.recordISelector.selectChannels(value);

        try {
            this.outputBuffer.emit(this.windowId, value, isRetract, targetChannels);
            this.outputMeter.mark();
        } catch (Exception e) {
            throw new GeaflowRuntimeException(e);
        }
    }

    /**
     * Shuffle data with key.
     */
    protected <KEY> void shuffle(KEY key, T value, boolean isRetract) {
        int[] targetChannels = this.recordISelector.selectChannels(key);

        try {
            this.outputBuffer.emit(this.windowId, value, isRetract, targetChannels);
            this.outputMeter.mark();
        } catch (Exception e) {
            throw new GeaflowRuntimeException(e);
        }
    }

}
