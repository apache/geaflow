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

package org.apache.geaflow.shuffle.serialize;

import java.util.ArrayList;
import java.util.List;
import org.apache.geaflow.model.record.impl.Record;
import org.apache.geaflow.shuffle.pipeline.buffer.HeapBuffer.HeapBufferBuilder;
import org.apache.geaflow.shuffle.pipeline.buffer.OutBuffer;
import org.apache.geaflow.shuffle.pipeline.buffer.OutBuffer.BufferBuilder;
import org.testng.Assert;
import org.testng.annotations.Test;

public class RecordSerializerTest {

    @Test
    public void test() {
        RecordSerializer serializer = new RecordSerializer();
        BufferBuilder outBuffer = new HeapBufferBuilder();
        List<Record> recordList = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            Record record = new Record(i);
            serializer.serialize(record, false, outBuffer);
            recordList.add(record);
        }
        OutBuffer buffer = outBuffer.build();
        MessageIterator deserializer = new MessageIterator(buffer);
        List<Record> list = new ArrayList<>();
        while (deserializer.hasNext()) {
            list.add((Record) deserializer.next());
        }
        Assert.assertEquals(recordList, list);
    }

}
