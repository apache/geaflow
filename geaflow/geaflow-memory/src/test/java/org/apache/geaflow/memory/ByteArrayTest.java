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

package org.apache.geaflow.memory;

import static org.apache.geaflow.memory.MemoryGroupManger.DEFAULT;

import com.google.common.collect.Maps;
import java.util.Map;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.memory.array.ByteArray;
import org.apache.geaflow.memory.array.ByteArrayBuilder;
import org.apache.geaflow.memory.config.MemoryConfigKeys;
import org.testng.Assert;
import org.testng.annotations.Test;

public class ByteArrayTest extends MemoryReleaseTest {

    @Test
    public void testMemoryView() {

        Map<String, String> conf = Maps.newHashMap();
        conf.put(MemoryConfigKeys.OFF_HEAP_MEMORY_SIZE_MB.getKey(), "128");

        MemoryManager.build(new Configuration(conf));

        MemoryView memoryView = MemoryManager.getInstance().requireMemory(1024, DEFAULT);


        String a = "MemoryView heapView = memoryManager.requireMemory(1025, MemoryMode.ON_HEAP";

        memoryView.getWriter().write(a.getBytes());

        MemoryViewReference reference = new MemoryViewReference(memoryView);

        ByteArray array1 = ByteArrayBuilder.of(reference, 0, memoryView.contentSize);

        int offset = memoryView.contentSize;

        String b = "MemoryView heapView = memoryManager.requireMemory(1025, MemoryMode.ON_HEAP)!";

        reference.getMemoryView().getWriter().write(b.getBytes());

        ByteArray array2 = ByteArrayBuilder.of(reference, offset, memoryView.contentSize - offset);

        Assert.assertEquals(a.getBytes(), array1.array());
        Assert.assertEquals(a.getBytes().length, array1.size());
        Assert.assertEquals(a.getBytes().length, array1.toByteBuffer().array().length);

        Assert.assertEquals(b.getBytes(), array2.array());
        Assert.assertEquals(b.getBytes().length, array2.size());

        array2.release();
        array1.release();

        Assert.assertNotNull(reference.getMemoryView());

        reference.decRef();
        Assert.assertNull(reference.getMemoryView());
    }


    @Test
    public void testDefaultArray() {

        String a = "MemoryView heapView = memoryManager.requireMemory(1025, MemoryMode.ON_HEAP";

        ByteArray array = ByteArrayBuilder.of(a.getBytes());

        Assert.assertNotNull(array.array());
        Assert.assertEquals(a.getBytes(), array.array());
        Assert.assertEquals(a.getBytes().length, array.size());
        Assert.assertEquals(a.getBytes().length, array.toByteBuffer().array().length);

        array.release();
        Assert.assertNull(array.array());
    }
}
