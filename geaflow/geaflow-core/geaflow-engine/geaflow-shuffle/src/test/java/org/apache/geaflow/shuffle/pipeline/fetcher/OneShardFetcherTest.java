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

package org.apache.geaflow.shuffle.pipeline.fetcher;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.common.config.keys.ExecutionConfigKeys;
import org.apache.geaflow.common.shuffle.ShuffleAddress;
import org.apache.geaflow.shuffle.message.PipelineSliceMeta;
import org.apache.geaflow.shuffle.message.SliceId;
import org.apache.geaflow.shuffle.network.IConnectionManager;
import org.apache.geaflow.shuffle.pipeline.buffer.HeapBuffer;
import org.apache.geaflow.shuffle.pipeline.buffer.PipeBuffer;
import org.apache.geaflow.shuffle.pipeline.buffer.PipeFetcherBuffer;
import org.apache.geaflow.shuffle.pipeline.channel.AbstractInputChannel;
import org.apache.geaflow.shuffle.pipeline.channel.LocalInputChannel;
import org.apache.geaflow.shuffle.pipeline.channel.RemoteInputChannel;
import org.apache.geaflow.shuffle.pipeline.slice.PipelineSlice;
import org.apache.geaflow.shuffle.pipeline.slice.SliceManager;
import org.apache.geaflow.shuffle.service.ShuffleManager;
import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

public class OneShardFetcherTest {

    private Configuration configuration;

    @BeforeTest
    public void setup() {
        configuration = new Configuration();
        configuration.put(ExecutionConfigKeys.JOB_APP_NAME, "default");
        configuration.put(ExecutionConfigKeys.CONTAINER_HEAP_SIZE_MB, String.valueOf(1024));
    }

    @Test
    public void testCreate() {
        List<PipelineSliceMeta> inputSlices = new ArrayList<>();
        IConnectionManager connectionManager = ShuffleManager.init(configuration).getConnectionManager();
        ShuffleAddress address = connectionManager.getShuffleAddress();
        PipelineSliceMeta slice1 = new PipelineSliceMeta(0, 0, -1, 0, address);
        PipelineSliceMeta slice2 = new PipelineSliceMeta(0, 2, -1, 0, address);
        inputSlices.add(slice1);
        inputSlices.add(slice2);

        OneShardFetcher fetcher = new OneShardFetcher(1, "taskName", 0, inputSlices, 0,
            connectionManager);
        Map<SliceId, AbstractInputChannel> channelMap = fetcher.getInputChannels();
        Assert.assertEquals(channelMap.size(), 2);

        Set<SliceId> expectedSlices = new HashSet<>();
        expectedSlices.add(new SliceId(-1, 0, 0, 0));
        expectedSlices.add(new SliceId(-1, 0, 0, 2));

        Set<Integer> expectedChannelIndices = new HashSet<>();
        expectedChannelIndices.add(0);
        expectedChannelIndices.add(1);

        Set<Integer> channelIds = new HashSet<>();
        for (Map.Entry<SliceId, AbstractInputChannel> entry : channelMap.entrySet()) {
            Assert.assertTrue(expectedSlices.remove(entry.getKey()));
            AbstractInputChannel channel = entry.getValue();
            channelIds.add(channel.getChannelIndex());
            Assert.assertTrue(channel instanceof LocalInputChannel);
        }
        Assert.assertEquals(expectedChannelIndices, channelIds);
    }

    @Test
    public void testRemoteFetch() throws IOException, InterruptedException {
        IConnectionManager connectionManager =
            ShuffleManager.init(configuration).getConnectionManager();
        ShuffleAddress address = connectionManager.getShuffleAddress();
        final SliceManager sliceManager = ShuffleManager.getInstance().getSliceManager();

        long pipelineId = 1;
        SliceId sliceId = new SliceId(pipelineId, 0, 0, 0);
        PipelineSliceMeta slice1 = new PipelineSliceMeta(sliceId, 1, address);
        List<PipelineSliceMeta> inputSlices = new ArrayList<>();
        inputSlices.add(slice1);

        OneShardFetcher fetcher = new MockedShardFetcher(1, "taskName", 0, inputSlices, 0,
            connectionManager);

        PipeBuffer pipeBuffer = new PipeBuffer(new HeapBuffer("hello".getBytes(), false), 1);
        PipeBuffer pipeBuffer2 = new PipeBuffer(1, 1, false);
        PipeBuffer pipeBuffer3 = new PipeBuffer(new HeapBuffer("hello".getBytes(), false), 2);
        PipeBuffer pipeBuffer4 = new PipeBuffer(2, 1, true);
        PipelineSlice slice = new PipelineSlice("task", sliceId);
        slice.add(pipeBuffer);
        slice.add(pipeBuffer2);
        slice.add(pipeBuffer3);
        slice.add(pipeBuffer4);

        sliceManager.register(sliceId, slice);

        List<Long> batchList = Arrays.asList(1L, 2L);
        List<String> result = new ArrayList<>();
        for (long batchId : batchList) {
            fetcher.requestSlices(batchId);
            while (!fetcher.isFinished()) {
                Optional<PipeFetcherBuffer> bufferOptional = fetcher.getNext();
                if (bufferOptional.isPresent()) {
                    PipeFetcherBuffer buffer = bufferOptional.get();
                    String value = String.valueOf(buffer.getBuffer());
                    result.add(value);
                    if (buffer.isBarrier()) {
                        break;
                    }
                }
            }
        }

        Assert.assertEquals(result.size(), 4);
        Assert.assertTrue(slice.canRelease());

        fetcher.close();
        for (AbstractInputChannel channel : fetcher.getInputChannels().values()) {
            if (channel instanceof RemoteInputChannel) {
                RemoteInputChannel remoteChannel = (RemoteInputChannel) channel;
                Assert.assertTrue(remoteChannel.getSliceRequestClient().disposeIfNotUsed());
            }
        }
        ShuffleManager.getInstance().release(pipelineId);
        ShuffleManager.getInstance().close();
    }

}
