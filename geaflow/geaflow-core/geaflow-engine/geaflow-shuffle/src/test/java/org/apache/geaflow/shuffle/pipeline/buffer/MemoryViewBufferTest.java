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

import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.memory.MemoryManager;
import org.apache.geaflow.shuffle.pipeline.buffer.MemoryViewBuffer.MemoryViewBufferBuilder;
import org.apache.geaflow.shuffle.serialize.MessageIterator;
import org.apache.geaflow.shuffle.serialize.RecordSerializer;
import org.testng.Assert;
import org.testng.annotations.Test;

public class MemoryViewBufferTest {

    @Test
    public void test() {
        Configuration configuration = new Configuration();
        MemoryManager manager = MemoryManager.build(configuration);

        int recordCount = 1024 * 16;
        MemoryViewBufferBuilder buffer = new MemoryViewBufferBuilder();
        RecordSerializer serializer = new RecordSerializer();
        for (int i = 0; i < recordCount; i++) {
            serializer.serialize(i, false, buffer);
        }

        Assert.assertTrue(buffer.getBufferSize() > 0);

        int result = 0;
        MessageIterator iterator = new MessageIterator(buffer.build().getInputStream());
        while (iterator.hasNext()) {
            iterator.next();
            result++;
        }
        Assert.assertTrue(result == recordCount);

        manager.dispose();
    }

}
