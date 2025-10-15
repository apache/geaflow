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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.nio.ByteBuffer;
import org.testng.Assert;
import org.testng.annotations.Test;

public class ChunkListTest extends MemoryReleaseTest {

    @Test
    public void test1() {

        AbstractMemoryPool pool = mock(AbstractMemoryPool.class);
        when(pool.getMemoryMode()).thenReturn(MemoryMode.ON_HEAP);

        ChunkList<ByteBuffer> c10 = new ChunkList<>(pool, 1, 10);
        ChunkList<ByteBuffer> c20 = new ChunkList<>(pool, 10, 100);

        c10.setPreList(null);
        c10.setNextList(c20);
        c20.setNextList(null);
        c20.setPreList(c10);

        int pageShift = MemoryManager.validateAndCalculatePageShifts(8192);

        Chunk<ByteBuffer> chunk = new Chunk<>(pool, new byte[ESegmentSize.S16777216.size()], 8192, 11, pageShift,
            ESegmentSize.S16777216.size(),
            0);
        long handle1 = chunk.allocate((int) (0.05 * ESegmentSize.S16777216.size()));

        c10.add(chunk);

        Assert.assertEquals(c10.getHead(), chunk);

        Chunk<ByteBuffer> chunk2 = new Chunk<>(pool, new byte[ESegmentSize.S16777216.size()], 8192, 11, pageShift,
            ESegmentSize.S16777216.size(),
            0);

        long handle2 = chunk2.allocate(ESegmentSize.S8388608.size());

        c10.add(chunk2);
        Assert.assertEquals(c20.getHead(), chunk2);

        c10.allocate(new ByteBuf<>(), (int) (0.1 * ESegmentSize.S16777216.size()));

        Assert.assertNull(c10.getHead());
        Assert.assertEquals(c20.getHead().next, chunk2);

        boolean needDestroy = !chunk2.parent.free(chunk2, handle2);

        Assert.assertTrue(needDestroy);

    }


    @Test
    public void test2() {

        AbstractMemoryPool pool = mock(AbstractMemoryPool.class);
        when(pool.getMemoryMode()).thenReturn(MemoryMode.OFF_HEAP);

        ChunkList<ByteBuffer> c10 = new ChunkList<>(pool, 1, 10);
        ChunkList<ByteBuffer> c20 = new ChunkList<>(pool, 10, 100);

        c10.setPreList(null);
        c10.setNextList(c20);
        c20.setNextList(null);
        c20.setPreList(c10);

        int pageShift = MemoryManager.validateAndCalculatePageShifts(8192);

        Chunk<ByteBuffer> chunk = new Chunk<>(pool,
            ByteBuffer.allocateDirect(ESegmentSize.S16777216.size()), 8192, 11, pageShift,
            ESegmentSize.S16777216.size(),
            0);
        long handle1 = chunk.allocate((int) (0.05 * ESegmentSize.S16777216.size()));

        c10.add(chunk);

        Assert.assertEquals(c10.getHead(), chunk);

        Chunk<ByteBuffer> chunk2 = new Chunk<>(pool,
            ByteBuffer.allocateDirect(ESegmentSize.S16777216.size()), 8192, 11, pageShift,
            ESegmentSize.S16777216.size(),
            0);

        long handle2 = chunk2.allocate(ESegmentSize.S8388608.size());

        c10.add(chunk2);
        Assert.assertEquals(c20.getHead(), chunk2);

        c10.allocate(new ByteBuf<>(), (int) (0.1 * ESegmentSize.S16777216.size()));

        Assert.assertNull(c10.getHead());
        Assert.assertEquals(c20.getHead().next, chunk2);

        boolean needDestroy = !chunk2.parent.free(chunk2, handle2);

        Assert.assertFalse(needDestroy);

        Chunk<ByteBuffer> chunk3 = new Chunk<>(pool, new byte[ESegmentSize.S16777216.size()], 8192, 11, pageShift,
            ESegmentSize.S16777216.size(),
            0);

        long handle3 = chunk3.allocate(ESegmentSize.S8388608.size());
        c10.add(chunk3);
        Assert.assertEquals(c20.getHead(), chunk3);
        Assert.assertEquals(chunk3.next, chunk);

        needDestroy = !chunk3.parent.free(chunk3, handle3);

        Assert.assertFalse(needDestroy);

    }
}
