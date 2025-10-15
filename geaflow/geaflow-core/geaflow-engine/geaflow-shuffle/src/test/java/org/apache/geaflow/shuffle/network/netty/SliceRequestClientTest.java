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

package org.apache.geaflow.shuffle.network.netty;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.common.config.keys.ExecutionConfigKeys;
import org.apache.geaflow.common.shuffle.ShuffleAddress;
import org.apache.geaflow.memory.MemoryGroupManger;
import org.apache.geaflow.memory.MemoryManager;
import org.apache.geaflow.memory.MemoryView;
import org.apache.geaflow.shuffle.message.PipelineSliceMeta;
import org.apache.geaflow.shuffle.message.SliceId;
import org.apache.geaflow.shuffle.network.IConnectionManager;
import org.apache.geaflow.shuffle.pipeline.buffer.MemoryViewBuffer;
import org.apache.geaflow.shuffle.pipeline.buffer.PipeBuffer;
import org.apache.geaflow.shuffle.pipeline.buffer.PipeFetcherBuffer;
import org.apache.geaflow.shuffle.pipeline.channel.AbstractInputChannel;
import org.apache.geaflow.shuffle.pipeline.channel.RemoteInputChannel;
import org.apache.geaflow.shuffle.pipeline.fetcher.MockedShardFetcher;
import org.apache.geaflow.shuffle.pipeline.fetcher.OneShardFetcher;
import org.apache.geaflow.shuffle.pipeline.slice.PipelineSlice;
import org.apache.geaflow.shuffle.pipeline.slice.SliceManager;
import org.apache.geaflow.shuffle.service.ShuffleManager;
import org.testng.Assert;
import org.testng.annotations.Factory;
import org.testng.annotations.Test;

@Test(singleThreaded = true)
public class SliceRequestClientTest {

    private boolean enableBackPressure;

    public SliceRequestClientTest(boolean enableBackPressure) {
        this.enableBackPressure = enableBackPressure;
    }

    @Test
    public void testFetchWithoutMemoryPool() throws IOException, InterruptedException {
        testCreditBasedFetch(false);
    }

    @Test
    public void testFetchWithMemoryPool() throws IOException, InterruptedException {
        testCreditBasedFetch(true);
    }

    private void testCreditBasedFetch(boolean enableMemoryPool) throws IOException,
        InterruptedException {
        Configuration configuration = new Configuration();
        configuration.put(ExecutionConfigKeys.JOB_APP_NAME, "default");
        configuration.put(ExecutionConfigKeys.CONTAINER_HEAP_SIZE_MB, String.valueOf(1024));
        configuration.put(ExecutionConfigKeys.SHUFFLE_BACKPRESSURE_ENABLE, String.valueOf(enableBackPressure));
        configuration.put(ExecutionConfigKeys.SHUFFLE_FETCH_CHANNEL_QUEUE_SIZE, "1");
        configuration.put(ExecutionConfigKeys.SHUFFLE_WRITER_BUFFER_SIZE, "10");
        configuration.put(ExecutionConfigKeys.SHUFFLE_FLUSH_BUFFER_SIZE_BYTES, "5");
        configuration.put(ExecutionConfigKeys.SHUFFLE_MEMORY_POOL_ENABLE, String.valueOf(enableMemoryPool));

        ShuffleManager shuffleManager = ShuffleManager.init(configuration);
        IConnectionManager connectionManager = shuffleManager.getConnectionManager();

        List<PipelineSliceMeta> inputSlices = new ArrayList<>();
        ShuffleAddress address = connectionManager.getShuffleAddress();
        SliceId sliceId = new SliceId(100, 0, 0, 0);
        PipelineSliceMeta slice1 = new PipelineSliceMeta(sliceId, 1, address);
        inputSlices.add(slice1);

        PipeBuffer pipeBuffer = buildPipeBuffer(enableMemoryPool, "hello".getBytes(), 1);
        PipeBuffer pipeBuffer2 = new PipeBuffer(1, 1, false);
        PipeBuffer pipeBuffer3 = buildPipeBuffer(enableMemoryPool, "hello".getBytes(), 2);
        PipeBuffer pipeBuffer4 = new PipeBuffer(2, 1, true);
        PipelineSlice slice = new PipelineSlice("task", sliceId);
        slice.add(pipeBuffer);
        slice.add(pipeBuffer2);
        slice.add(pipeBuffer3);
        slice.add(pipeBuffer4);

        SliceManager shuffleDataManager = ShuffleManager.getInstance().getSliceManager();
        shuffleDataManager.register(sliceId, slice);

        OneShardFetcher fetcher = new MockedShardFetcher(1, "taskName", 0, inputSlices, 0,
            connectionManager);
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

        ShuffleManager.getInstance().release(sliceId.getPipelineId());
        ShuffleManager.getInstance().close();
    }

    private PipeBuffer buildPipeBuffer(boolean enableMemoryPool, byte[] content, long batchId) {
        if (enableMemoryPool) {
            MemoryView view = MemoryManager.getInstance()
                .requireMemory(content.length, MemoryGroupManger.SHUFFLE);
            view.getWriter().write(content);
            return new PipeBuffer(new MemoryViewBuffer(view, false), batchId);
        } else {
            return new PipeBuffer(content, batchId);
        }
    }

    public static class SimpleTestFactory {

        @Factory
        public Object[] factoryMethod() {
            return new Object[]{
                new SliceRequestClientTest(false),
                new SliceRequestClientTest(true),
            };
        }
    }

}