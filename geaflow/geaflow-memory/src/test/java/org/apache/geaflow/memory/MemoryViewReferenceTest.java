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
import org.apache.geaflow.memory.config.MemoryConfigKeys;
import org.testng.Assert;
import org.testng.annotations.Test;

public class MemoryViewReferenceTest extends MemoryReleaseTest {


    @Test
    public void test() {

        Map<String, String> conf = Maps.newHashMap();
        conf.put(MemoryConfigKeys.OFF_HEAP_MEMORY_SIZE_MB.getKey(), "128");

        MemoryManager.build(new Configuration(conf));

        MemoryView memoryView = MemoryManager.getInstance().requireMemory(1023, DEFAULT);

        MemoryViewReference reference = new MemoryViewReference(memoryView);

        reference.incRef();
        reference.incRef();
        Assert.assertNotNull(reference.getMemoryView());

        reference.decRef();
        reference.decRef();

        Assert.assertNotNull(reference.getMemoryView());

        reference.decRef();

        Assert.assertNull(reference.getMemoryView());
    }
}
