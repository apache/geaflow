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

import com.google.common.collect.Maps;
import java.util.Map;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.common.utils.MemoryUtils;
import org.apache.geaflow.memory.config.MemoryConfigKeys;
import org.testng.Assert;
import org.testng.annotations.Test;

public class MemoryGroupTest extends MemoryReleaseTest {


    @Test
    public void test1() {

        Map<String, String> conf = Maps.newHashMap();
        conf.put(MemoryConfigKeys.OFF_HEAP_MEMORY_SIZE_MB.getKey(), "32");
        MemoryManager.build(new Configuration(conf));

        MemoryView view = MemoryManager.getInstance()
            .requireMemory(1024, MemoryGroupManger.DEFAULT);

        Assert.assertEquals(MemoryGroupManger.DEFAULT.usedBytes(), 1024);
        System.out.println(MemoryGroupManger.DEFAULT.toString());

        Assert.assertEquals(MemoryGroupManger.DEFAULT, new MemoryGroup("default", 1024));

        view.reset();

        Assert.assertEquals(MemoryGroupManger.DEFAULT.usedBytes(), 1024);

        view.close();

        Assert.assertEquals(MemoryGroupManger.DEFAULT.usedBytes(), 0);

        MemoryManager.getInstance().dispose();

        Assert.assertEquals(MemoryGroupManger.DEFAULT.getThreads(), 0);
    }

    @Test
    public void test2() {

        Map<String, String> conf = Maps.newHashMap();
        conf.put(MemoryConfigKeys.OFF_HEAP_MEMORY_SIZE_MB.getKey(), "32");
        MemoryManager.build(new Configuration(conf));

        MemoryView view = MemoryManager.getInstance()
            .requireMemory(1024, MemoryGroupManger.DEFAULT);

        Assert.assertEquals(MemoryGroupManger.DEFAULT.usedBytes(), 1024);

        MemoryViewWriter writer = view.getWriter(1024);
        writer.write(new byte[2048]);

        Assert.assertEquals(MemoryGroupManger.DEFAULT.usedBytes(), 2048);

        writer.write(new byte[1]);

        Assert.assertEquals(MemoryGroupManger.DEFAULT.usedBytes(), 3072);

        view.close();

        Assert.assertEquals(MemoryGroupManger.DEFAULT.usedBytes(), 0);
    }

    @Test
    public void test3() {

        long memory = 32 * MemoryUtils.MB;

        MemoryGroupManger.getInstance().resetMemory(memory, (int) (1 * MemoryUtils.MB),
            MemoryMode.OFF_HEAP);

        long shuffleBase = 4 * MemoryUtils.MB;

        Assert.assertEquals(MemoryGroupManger.DEFAULT.baseBytes(), 0);
        Assert.assertEquals(MemoryGroupManger.STATE.baseBytes(), 0);
        Assert.assertEquals(MemoryGroupManger.SHUFFLE.baseBytes(), shuffleBase);

        Assert.assertEquals(MemoryGroupManger.getInstance().getCurrentSharedFreeBytes(),
            memory - shuffleBase);

        Assert.assertEquals(MemoryGroupManger.SHUFFLE.allocate(2048, MemoryMode.OFF_HEAP), true);
        Assert.assertEquals(MemoryGroupManger.SHUFFLE.usedBytes(), 2048);
        Assert.assertEquals(MemoryGroupManger.SHUFFLE.allocate(shuffleBase, MemoryMode.OFF_HEAP),
            true);
        Assert.assertEquals(MemoryGroupManger.SHUFFLE.usedBytes(), shuffleBase + 2048);

        Assert.assertEquals(MemoryGroupManger.getInstance().getCurrentSharedFreeBytes(),
            28 * MemoryUtils.MB - shuffleBase);

        Assert.assertEquals(MemoryGroupManger.SHUFFLE.allocate(1024, MemoryMode.OFF_HEAP), true);

        MemoryGroupManger.DEFAULT.allocate(28 * MemoryUtils.MB - shuffleBase, MemoryMode.OFF_HEAP);
        MemoryGroupManger.SHUFFLE.allocate(shuffleBase - 3072, MemoryMode.OFF_HEAP);

        Assert.assertEquals(MemoryGroupManger.getInstance().getCurrentSharedFreeBytes(), 0);

        Assert.assertEquals(MemoryGroupManger.DEFAULT.allocate(1, MemoryMode.OFF_HEAP), false);
        Assert.assertEquals(MemoryGroupManger.SHUFFLE.allocate(1, MemoryMode.OFF_HEAP), false);

        Assert.assertEquals(MemoryGroupManger.SHUFFLE.usedBytes(), 8 * MemoryUtils.MB);
        Assert.assertEquals(MemoryGroupManger.DEFAULT.usedBytes(), 24 * MemoryUtils.MB);

        Assert.assertEquals(MemoryGroupManger.SHUFFLE.free(shuffleBase, MemoryMode.OFF_HEAP), true);
        Assert.assertEquals(MemoryGroupManger.getInstance().getCurrentSharedFreeBytes(), 0);
        Assert.assertEquals(MemoryGroupManger.SHUFFLE.free(shuffleBase, MemoryMode.OFF_HEAP), true);
        Assert.assertEquals(MemoryGroupManger.getInstance().getCurrentSharedFreeBytes(), shuffleBase);

        MemoryGroupManger.getInstance().clear();

        Assert.assertEquals(MemoryGroupManger.SHUFFLE.usedBytes(), 0);
    }

    @Test
    public void test4() {
        long memory = 32 * MemoryUtils.MB;
        MemoryGroupManger.getInstance().resetMemory(memory, (int) (1 * MemoryUtils.MB), MemoryMode.OFF_HEAP);
        long shuffleBase = 4 * MemoryUtils.MB;

        Assert.assertEquals(MemoryGroupManger.getInstance().getCurrentSharedFreeBytes(),
            memory - shuffleBase);

        Assert.assertTrue(MemoryGroupManger.DEFAULT.allocate(2048, MemoryMode.OFF_HEAP));
        Assert.assertEquals(MemoryGroupManger.DEFAULT.usedBytes(), 2048);
        Assert.assertTrue(MemoryGroupManger.STATE.allocate(2048, MemoryMode.OFF_HEAP));
        Assert.assertEquals(MemoryGroupManger.STATE.usedBytes(), 2048);

        Assert.assertEquals(MemoryGroupManger.getInstance().getCurrentSharedFreeBytes(),
            memory - shuffleBase - 4096);

        Assert.assertTrue(
            MemoryGroupManger.STATE.allocate(MemoryGroupManger.getInstance().getCurrentSharedFreeBytes(),
                MemoryMode.OFF_HEAP));

        Assert.assertEquals(MemoryGroupManger.getInstance().getCurrentSharedFreeBytes(), 0);

        Assert.assertFalse(MemoryGroupManger.DEFAULT.allocate(2048, MemoryMode.OFF_HEAP));
        Assert.assertFalse(MemoryGroupManger.STATE.allocate(2048, MemoryMode.OFF_HEAP));

        MemoryGroupManger.getInstance().clear();
    }

    @Test
    public void test5() {
        long memory = 32 * MemoryUtils.MB;
        MemoryGroupManger.getInstance().resetMemory(memory, (int) (1 * MemoryUtils.MB), MemoryMode.ON_HEAP);

        Assert.assertTrue(MemoryGroupManger.DEFAULT.allocate(2048, MemoryMode.ON_HEAP));
        Assert.assertTrue(MemoryGroupManger.STATE.allocate(2048, MemoryMode.ON_HEAP));
        Assert.assertTrue(MemoryGroupManger.SHUFFLE.allocate(2048, MemoryMode.ON_HEAP));

        Assert.assertEquals(MemoryGroupManger.DEFAULT.usedBytes(), 2048);
        Assert.assertEquals(MemoryGroupManger.SHUFFLE.usedBytes(), 2048);
        Assert.assertEquals(MemoryGroupManger.STATE.usedBytes(), 2048);

        Assert.assertTrue(MemoryGroupManger.SHUFFLE.allocate(memory, MemoryMode.ON_HEAP));

        Assert.assertEquals(MemoryGroupManger.SHUFFLE.usedBytes(), memory + 2048);


        Assert.assertTrue(MemoryGroupManger.DEFAULT.allocate(memory, MemoryMode.ON_HEAP));

        Assert.assertEquals(MemoryGroupManger.DEFAULT.usedBytes(), memory + 2048);

        Assert.assertEquals(MemoryGroupManger.DEFAULT.usedOffHeapBytes(), 0);
        Assert.assertEquals(MemoryGroupManger.DEFAULT.usedOnHeapBytes(), memory + 2048);

        Assert.assertTrue(MemoryGroupManger.DEFAULT.free(memory, MemoryMode.ON_HEAP));
        Assert.assertEquals(MemoryGroupManger.DEFAULT.usedBytes(), 2048);
    }

    @Test
    public void testRatio() {
        MemoryGroupManger.getInstance().clear();

        MemoryGroupManger.getInstance().load(new Configuration());
        Assert.assertEquals(MemoryGroupManger.SHUFFLE.getRatio(), 0.1);
        Assert.assertEquals(MemoryGroupManger.STATE.getRatio(), 0.0);
        Assert.assertEquals(MemoryGroupManger.DEFAULT.getRatio(), 0.0);

        long memory = 32 * MemoryUtils.MB;
        MemoryGroupManger.getInstance().resetMemory(memory, (int) (1 * MemoryUtils.MB), MemoryMode.OFF_HEAP);
        long shuffleBase = 4 * MemoryUtils.MB;

        Assert.assertEquals(MemoryGroupManger.getInstance().getCurrentSharedFreeBytes(),
            memory - shuffleBase);

        Assert.assertTrue(MemoryGroupManger.DEFAULT.allocate(2048, MemoryMode.OFF_HEAP));
        Assert.assertEquals(MemoryGroupManger.DEFAULT.usedBytes(), 2048);
        Assert.assertTrue(MemoryGroupManger.STATE.allocate(2048, MemoryMode.OFF_HEAP));
        Assert.assertEquals(MemoryGroupManger.STATE.usedBytes(), 2048);

        Assert.assertEquals(MemoryGroupManger.getInstance().getCurrentSharedFreeBytes(),
            memory - shuffleBase - 4096);

        Assert.assertTrue(
            MemoryGroupManger.STATE.allocate(MemoryGroupManger.getInstance().getCurrentSharedFreeBytes(),
                MemoryMode.OFF_HEAP));

        Assert.assertEquals(MemoryGroupManger.getInstance().getCurrentSharedFreeBytes(), 0);

        Assert.assertFalse(MemoryGroupManger.DEFAULT.allocate(2048, MemoryMode.OFF_HEAP));
        Assert.assertFalse(MemoryGroupManger.STATE.allocate(2048, MemoryMode.OFF_HEAP));

        MemoryGroupManger.getInstance().clear();

    }

    @Test
    public void testRatio2() {
        MemoryGroupManger.getInstance().clear();

        Map<String, String> config = Maps.newHashMap();
        config.put(MemoryConfigKeys.MEMORY_GROUP_RATIO.getKey(), "10:20:*");

        MemoryGroupManger.getInstance().load(new Configuration(config));
        Assert.assertEquals(MemoryGroupManger.SHUFFLE.getRatio(), 0.1);
        Assert.assertEquals(MemoryGroupManger.STATE.getRatio(), 0.2);
        Assert.assertEquals(MemoryGroupManger.DEFAULT.getRatio(), 0.0);

        long memory = 32 * MemoryUtils.MB;
        MemoryGroupManger.getInstance().resetMemory(memory, (int) (1 * MemoryUtils.MB), MemoryMode.OFF_HEAP);
        long shuffleBase = 4 * MemoryUtils.MB;
        long stateBase = 7 * MemoryUtils.MB;

        Assert.assertEquals(MemoryGroupManger.getInstance().getCurrentSharedFreeBytes(),
            memory - shuffleBase - stateBase);

        Assert.assertTrue(MemoryGroupManger.DEFAULT.allocate(2048, MemoryMode.OFF_HEAP));
        Assert.assertEquals(MemoryGroupManger.DEFAULT.usedBytes(), 2048);
        Assert.assertTrue(MemoryGroupManger.STATE.allocate(2048, MemoryMode.OFF_HEAP));
        Assert.assertEquals(MemoryGroupManger.STATE.usedBytes(), 2048);

        Assert.assertEquals(MemoryGroupManger.getInstance().getCurrentSharedFreeBytes(),
            memory - shuffleBase - stateBase - 2048);

        Assert.assertTrue(
            MemoryGroupManger.DEFAULT.allocate(MemoryGroupManger.getInstance().getCurrentSharedFreeBytes(),
                MemoryMode.OFF_HEAP));

        Assert.assertEquals(MemoryGroupManger.getInstance().getCurrentSharedFreeBytes(), 0);

        Assert.assertFalse(MemoryGroupManger.DEFAULT.allocate(2048, MemoryMode.OFF_HEAP));
        Assert.assertTrue(MemoryGroupManger.STATE.allocate(2048, MemoryMode.OFF_HEAP));

        Assert.assertEquals(MemoryGroupManger.STATE.usedBytes(), 4096);

        MemoryGroupManger.getInstance().clear();

    }

    @Test
    public void testRatio3() {
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            Map<String, String> config = Maps.newHashMap();
            config.put(MemoryConfigKeys.MEMORY_GROUP_RATIO.getKey(), "10:20");

            MemoryGroupManger.getInstance().load(new Configuration(config));
        });
    }

    @Test
    public void testRatio4() {
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            Map<String, String> config = Maps.newHashMap();
            config.put(MemoryConfigKeys.MEMORY_GROUP_RATIO.getKey(), "10:200:*");

            MemoryGroupManger.getInstance().load(new Configuration(config));
        });
    }
}
