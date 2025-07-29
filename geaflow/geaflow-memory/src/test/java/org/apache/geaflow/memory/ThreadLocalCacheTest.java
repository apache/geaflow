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
import static org.apache.geaflow.memory.MemoryGroupManger.SHUFFLE;

import com.google.common.collect.Maps;
import java.util.List;
import java.util.Map;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.memory.config.MemoryConfigKeys;
import org.testng.Assert;
import org.testng.annotations.Test;

public class ThreadLocalCacheTest extends MemoryReleaseTest {

    @Test
    public void test1() {

        Map<String, String> conf = Maps.newHashMap();
        conf.put(MemoryConfigKeys.OFF_HEAP_MEMORY_SIZE_MB.getKey(), "96");
        conf.put(MemoryConfigKeys.MAX_DIRECT_MEMORY_SIZE.getKey(), "" + 96 * 1024 * 1024);
        conf.put(MemoryConfigKeys.MEMORY_PAGE_SIZE.getKey(), "" + 8 * 1024);
        conf.put(MemoryConfigKeys.MEMORY_MAX_ORDER.getKey(), "11");
        conf.put(MemoryConfigKeys.MEMORY_POOL_SIZE.getKey(), "6");

        MemoryManager.build(new Configuration(conf));

        List<ByteBuf> byteBufs = MemoryManager.getInstance()
            .requireBufs(ESegmentSize.largest().size(), MemoryGroupManger.DEFAULT);
        Assert.assertEquals(byteBufs.size(), 1);

        AbstractMemoryPool pool = byteBufs.get(0).chunk.pool;

        byteBufs = MemoryManager.getInstance()
            .requireBufs(ESegmentSize.largest().size(), MemoryGroupManger.DEFAULT);
        Assert.assertEquals(byteBufs.size(), 1);

        AbstractMemoryPool pool2 = byteBufs.get(0).chunk.pool;
        Assert.assertNotSame(pool, pool2);

        byteBufs = MemoryManager.getInstance()
            .requireBufs(ESegmentSize.smallest().size(), MemoryGroupManger.DEFAULT);
        Assert.assertEquals(byteBufs.size(), 1);

        AbstractMemoryPool pool3 = byteBufs.get(0).chunk.pool;
        Assert.assertNotSame(pool3, pool2);

        byteBufs = MemoryManager.getInstance()
            .requireBufs(ESegmentSize.smallest().size(), MemoryGroupManger.DEFAULT);
        Assert.assertEquals(byteBufs.size(), 1);

        AbstractMemoryPool pool4 = byteBufs.get(0).chunk.pool;
        Assert.assertEquals(pool3, pool4);

        byteBufs = MemoryManager.getInstance()
            .requireBufs(ESegmentSize.largest().size() + ESegmentSize.smallest().size(),
                MemoryGroupManger.DEFAULT);
        Assert.assertEquals(byteBufs.size(), 2);

        Assert.assertNotSame(byteBufs.get(0).chunk.pool, byteBufs.get(1).chunk.pool);

        try {
            byteBufs = MemoryManager.getInstance()
                .requireBufs(ESegmentSize.largest().size() + ESegmentSize.smallest().size(),
                    MemoryGroupManger.DEFAULT);
        } catch (Throwable t) {
            Assert.assertTrue(t instanceof OutOfMemoryError);
        }

    }

    @Test
    public void test2() {

        Map<String, String> conf = Maps.newHashMap();
        conf.put(MemoryConfigKeys.OFF_HEAP_MEMORY_SIZE_MB.getKey(), "48");
        conf.put(MemoryConfigKeys.MAX_DIRECT_MEMORY_SIZE.getKey(), "" + 48 * 1024 * 1024);
        conf.put(MemoryConfigKeys.MEMORY_PAGE_SIZE.getKey(), "" + 8 * 1024);
        conf.put(MemoryConfigKeys.MEMORY_MAX_ORDER.getKey(), "11");
        conf.put(MemoryConfigKeys.MEMORY_POOL_SIZE.getKey(), "3");

        MemoryManager.build(new Configuration(conf));

        ByteBuf byteBuf = MemoryManager.getInstance()
            .requireBuf(ESegmentSize.largest().size(), SHUFFLE);

        AbstractMemoryPool pool = byteBuf.chunk.pool;

        byteBuf = MemoryManager.getInstance().requireBuf(ESegmentSize.largest().size(), SHUFFLE);

        AbstractMemoryPool pool2 = byteBuf.chunk.pool;
        Assert.assertNotSame(pool, pool2);

        byteBuf = MemoryManager.getInstance().requireBuf(ESegmentSize.smallest().size(), SHUFFLE);

        AbstractMemoryPool pool3 = byteBuf.chunk.pool;
        Assert.assertNotSame(pool3, pool2);

        byteBuf = MemoryManager.getInstance().requireBuf(ESegmentSize.smallest().size(), SHUFFLE);

        AbstractMemoryPool pool4 = byteBuf.chunk.pool;
        Assert.assertEquals(pool3, pool4);

        try {
            byteBuf = MemoryManager.getInstance()
                .requireBuf(ESegmentSize.largest().size(), SHUFFLE);
        } catch (Throwable t) {
            Assert.assertTrue(t instanceof OutOfMemoryError);
        }

    }

    @Test
    public void test3() {
        Map<String, String> conf = Maps.newHashMap();
        conf.put(MemoryConfigKeys.OFF_HEAP_MEMORY_SIZE_MB.getKey(), "16");
        conf.put(MemoryConfigKeys.MAX_DIRECT_MEMORY_SIZE.getKey(), "" + 16 * 1024 * 1024);
        conf.put(MemoryConfigKeys.MEMORY_PAGE_SIZE.getKey(), "" + 8 * 1024);
        conf.put(MemoryConfigKeys.MEMORY_MAX_ORDER.getKey(), "11");
        conf.put(MemoryConfigKeys.MEMORY_POOL_SIZE.getKey(), "1");
        conf.put(MemoryConfigKeys.MEMORY_AUTO_ADAPT_ENABLE.getKey(), "true");

        MemoryManager.build(new Configuration(conf));

        ByteBuf byteBuf = MemoryManager.getInstance()
            .requireBuf(ESegmentSize.largest().size(), SHUFFLE);
        Assert.assertEquals(byteBuf.chunk.pool.getMemoryMode(), MemoryMode.OFF_HEAP);

        byteBuf = MemoryManager.getInstance().requireBuf(ESegmentSize.largest().size(), SHUFFLE);

        Assert.assertEquals(byteBuf.chunk.pool.getMemoryMode(), MemoryMode.ON_HEAP);

        byteBuf.free(SHUFFLE);

    }

    @Test
    public void test4() {
        Map<String, String> conf = Maps.newHashMap();
        conf.put(MemoryConfigKeys.OFF_HEAP_MEMORY_SIZE_MB.getKey(), "16");
        conf.put(MemoryConfigKeys.MAX_DIRECT_MEMORY_SIZE.getKey(), "" + 16 * 1024 * 1024);
        conf.put(MemoryConfigKeys.MEMORY_PAGE_SIZE.getKey(), "" + 8 * 1024);
        conf.put(MemoryConfigKeys.MEMORY_MAX_ORDER.getKey(), "11");
        conf.put(MemoryConfigKeys.MEMORY_POOL_SIZE.getKey(), "1");
        conf.put(MemoryConfigKeys.MEMORY_AUTO_ADAPT_ENABLE.getKey(), "true");

        MemoryManager.build(new Configuration(conf));

        List<ByteBuf> byteBufs = MemoryManager.getInstance()
            .requireBufs(ESegmentSize.largest().size(), DEFAULT);
        Assert.assertEquals(byteBufs.size(), 1);

        Assert.assertEquals(byteBufs.get(0).chunk.pool.getMemoryMode(), MemoryMode.ON_HEAP);
        byteBufs.get(0).free(SHUFFLE);

        byteBufs = MemoryManager.getInstance().requireBufs(ESegmentSize.largest().size(), SHUFFLE);
        Assert.assertEquals(byteBufs.size(), 1);

        Assert.assertEquals(byteBufs.get(0).chunk.pool.getMemoryMode(), MemoryMode.OFF_HEAP);

        byteBufs = MemoryManager.getInstance().requireBufs(ESegmentSize.largest().size(), SHUFFLE);

        Assert.assertEquals(byteBufs.size(), 1);

        Assert.assertEquals(byteBufs.get(0).chunk.pool.getMemoryMode(), MemoryMode.ON_HEAP);

        byteBufs.get(0).free(SHUFFLE);

    }
}
