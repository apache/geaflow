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

import static org.apache.geaflow.common.config.keys.ExecutionConfigKeys.SHUFFLE_SPILL_RECORDS;

import java.io.IOException;
import java.util.UUID;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.common.config.keys.ExecutionConfigKeys;
import org.apache.geaflow.shuffle.config.ShuffleConfig;
import org.apache.geaflow.shuffle.message.SliceId;
import org.apache.geaflow.shuffle.pipeline.buffer.HeapBuffer;
import org.apache.geaflow.shuffle.pipeline.buffer.PipeBuffer;
import org.apache.geaflow.shuffle.pipeline.buffer.PipeChannelBuffer;
import org.apache.geaflow.shuffle.pipeline.buffer.ShuffleMemoryTracker;
import org.testng.Assert;
import org.testng.annotations.Test;

public class SpillablePipelineSliceTest {

    @Test
    public void testAdd() throws IOException {
        Configuration config = new Configuration();
        config.put(SHUFFLE_SPILL_RECORDS, "50");
        config.put(ExecutionConfigKeys.CONTAINER_HEAP_SIZE_MB, String.valueOf(1024));

        long id = UUID.randomUUID().getLeastSignificantBits();
        SliceId sliceId = new SliceId(id, 0, 0, 0);

        ShuffleConfig shuffleConfig = new ShuffleConfig(config);
        ShuffleMemoryTracker memoryTracker = new ShuffleMemoryTracker(config);
        SpillablePipelineSlice slice = new SpillablePipelineSlice("test", sliceId, shuffleConfig,
            memoryTracker);

        byte[] bytes1 = new byte[100];

        int bufferCount = 10000;
        for (int i = 0; i < bufferCount; i++) {
            HeapBuffer outBuffer = new HeapBuffer(bytes1, memoryTracker);
            PipeBuffer buffer = new PipeBuffer(outBuffer, 1);
            slice.add(buffer);
        }
        slice.flush();

        // Check repeatable reader.
        int consumedBufferCount = 0;
        PipelineSliceReader reader = slice.createSliceReader(1, () -> {
        });
        while (reader.hasNext()) {
            PipeChannelBuffer buffer = reader.next();
            if (buffer != null) {
                consumedBufferCount++;
            }
        }

        Assert.assertEquals(bufferCount, consumedBufferCount);
        Assert.assertTrue(slice.canRelease());
        slice.release();
    }

}
