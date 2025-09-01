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

import java.io.IOException;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.common.config.keys.ExecutionConfigKeys;
import org.apache.geaflow.shuffle.config.ShuffleConfig;
import org.apache.geaflow.shuffle.message.SliceId;
import org.apache.geaflow.shuffle.pipeline.buffer.PipeBuffer;
import org.apache.geaflow.shuffle.pipeline.buffer.ShuffleMemoryTracker;
import org.apache.geaflow.shuffle.pipeline.slice.BlockingSlice;
import org.apache.geaflow.shuffle.pipeline.slice.IPipelineSlice;
import org.apache.geaflow.shuffle.pipeline.slice.PipelineSlice;
import org.apache.geaflow.shuffle.pipeline.slice.SliceManager;
import org.apache.geaflow.shuffle.service.ShuffleManager;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Factory;
import org.testng.annotations.Test;

public class PipelineShardWriterTest {

    private boolean enableBackPressure;

    public PipelineShardWriterTest(boolean enableBackPressure) {
        this.enableBackPressure = enableBackPressure;
    }

    @Test
    public void testWrite() throws IOException, InterruptedException {
        Configuration configuration = new Configuration();
        configuration.put(ExecutionConfigKeys.JOB_APP_NAME, "default");
        configuration.put(ExecutionConfigKeys.CONTAINER_HEAP_SIZE_MB, String.valueOf(1024));
        configuration.put(ExecutionConfigKeys.SHUFFLE_BACKPRESSURE_ENABLE,
            String.valueOf(enableBackPressure));
        configuration.put(ExecutionConfigKeys.SHUFFLE_FETCH_CHANNEL_QUEUE_SIZE, "1");
        configuration.put(ExecutionConfigKeys.SHUFFLE_WRITER_BUFFER_SIZE, "20");
        configuration.put(ExecutionConfigKeys.SHUFFLE_FLUSH_BUFFER_SIZE_BYTES, "10");

        int pipelineId = 300;
        SliceManager sliceManager = new SliceManager();
        ShuffleConfig shuffleConfig = new ShuffleConfig(configuration);
        ShuffleMemoryTracker memoryTracker = new ShuffleMemoryTracker(configuration);

        try (MockedStatic<ShuffleManager> ms = Mockito.mockStatic(ShuffleManager.class)) {
            ShuffleManager shuffleManager = Mockito.mock(ShuffleManager.class);
            ms.when(() -> ShuffleManager.getInstance()).then(invocation -> shuffleManager);

            Mockito.doReturn(sliceManager).when(shuffleManager).getSliceManager();
            Mockito.doReturn(shuffleConfig).when(shuffleManager).getShuffleConfig();
            Mockito.doReturn(memoryTracker).when(shuffleManager).getShuffleMemoryTracker();

            WriterContext writerContext = new WriterContext(pipelineId, "write-test");
            writerContext.setConfig(shuffleConfig);
            writerContext.setChannelNum(1);
            writerContext.setEdgeId(0);

            PipelineShardWriter shardWriter = new PipelineShardWriter();
            shardWriter.init(writerContext);

            new Thread(() -> {
                IPipelineSlice slice = sliceManager.getSlice(new SliceId(pipelineId, 0, 0, 0));
                Assert.assertNotNull(slice);
                if (enableBackPressure) {
                    Assert.assertTrue(slice instanceof BlockingSlice);
                } else {
                    Assert.assertTrue(slice instanceof PipelineSlice);
                }

                int count = 0;
                while (true) {
                    PipeBuffer buffer = slice.next();
                    if (buffer != null) {
                        if (!buffer.isData()) {
                            break;
                        }
                        count++;
                    }
                }

                Assert.assertEquals(count, 10000);
            }).start();

            int[] channels = new int[]{0};
            for (int i = 0; i < 10000; i++) {
                shardWriter.emit(0, "helloWorld", false, channels);
            }
            shardWriter.finish(0);

            ShuffleManager.getInstance().release(pipelineId);
            Mockito.reset(shuffleManager);
        }
    }

    public static class SimpleTestFactory {

        @Factory
        public Object[] factoryMethod() {
            return new Object[]{
                new PipelineShardWriterTest(false),
                new PipelineShardWriterTest(true),
            };
        }
    }

}