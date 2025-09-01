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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.geaflow.memory.metric.ChunkListMetric;
import org.apache.geaflow.memory.metric.PoolMetric;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractMemoryPool<T> implements PoolMetric {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractMemoryPool.class);

    final int maxOrder;
    final int pageSize;
    final int pageShifts;
    final int chunkSize;

    protected final ChunkList<T> q050;
    protected final ChunkList<T> q025;
    protected final ChunkList<T> q000;
    protected final ChunkList<T> qInit;
    protected final ChunkList<T> q075;
    protected final ChunkList<T> q100;

    protected final List<ChunkListMetric> chunkListMetrics = Lists.newArrayList();

    private Map<ESegmentSize, Page<T>> subpages = Maps.newHashMap();

    Map<MemoryGroup, AtomicInteger> groupThreadCounter = Maps.newHashMap();

    final MemoryManager memoryManager;
    private AtomicLong usedMemory;

    protected int currentChunkNum = 0;
    protected long allocateMemory;

    public AbstractMemoryPool(MemoryManager memoryManager, int pageSize, int maxOrder, int pageShifts, int chunkSize) {
        this.pageSize = pageSize;
        this.maxOrder = maxOrder;
        this.pageShifts = pageShifts;
        this.chunkSize = chunkSize;
        this.memoryManager = memoryManager;

        q100 = new ChunkList<T>(this, 100, Integer.MAX_VALUE);
        q075 = new ChunkList<T>(this, 75, 100);
        q050 = new ChunkList<T>(this, 50, 100);
        q025 = new ChunkList<T>(this, 25, 75);
        q000 = new ChunkList<T>(this, 1, 50);
        qInit = new ChunkList<T>(this, Integer.MIN_VALUE, 25);

        q100.setPreList(q075);
        q100.setNextList(null);
        q075.setPreList(q050);
        q075.setNextList(q100);
        q050.setPreList(q025);
        q050.setNextList(q075);
        q025.setPreList(q000);
        q025.setNextList(q050);
        q000.setPreList(null);
        q000.setNextList(q025);
        qInit.setPreList(qInit);
        qInit.setNextList(q000);

        ESegmentSize slot = ESegmentSize.valueOf(pageSize);
        for (int i = 0; i <= slot.index(); i++) {
            subpages.put(ESegmentSize.upValues[i], newPageHead(pageSize));
        }

        chunkListMetrics.add(qInit);
        chunkListMetrics.add(q000);
        chunkListMetrics.add(q025);
        chunkListMetrics.add(q050);
        chunkListMetrics.add(q075);
        chunkListMetrics.add(q100);

        usedMemory = new AtomicLong(0);
    }

    private Page<T> newPageHead(int pageSize) {
        Page<T> head = new Page<>(pageSize);
        head.prev = head;
        head.next = head;
        return head;
    }

    List<ByteBuf<T>> allocate(int reqCapacity, MemoryGroup group) {
        int size = 1;
        if (reqCapacity > chunkSize) {
            size = (reqCapacity / chunkSize) + (reqCapacity % chunkSize > 0 ? 1 : 0);
        }
        List<ByteBuf<T>> bufs = new ArrayList<>(size);
        boolean failed = false;
        for (int i = 0; i < size - 1; i++) {
            ByteBuf<T> buf = oneAllocate(chunkSize, group);
            if (buf.allocateFailed()) {
                failed = true;
                break;
            }
            bufs.add(buf);
        }
        if (!failed) {
            ByteBuf<T> buf = oneAllocate(reqCapacity - (size - 1) * chunkSize, group);
            if (!buf.allocateFailed()) {
                bufs.add(buf);
            }
        }
        return bufs;
    }

    ByteBuf<T> oneAllocate(int reqCapacity, MemoryGroup group) {
        final int normCapacity = normalizeCapacity(reqCapacity);
        ByteBuf<T> byteBuf = newByteBuf(normCapacity);
        allocate(byteBuf, normCapacity, group);
        usedMemory.getAndAdd(byteBuf.length);
        return byteBuf;
    }

    private void allocate(ByteBuf<T> byteBuf, final int normCapacity, MemoryGroup group) {

        if (normCapacity < pageSize) {
            ESegmentSize slot = normalize(normCapacity);

            final Page<T> head = subpages.get(slot);
            synchronized (head) {
                final Page<T> s = head.next;
                if (s != head) {
                    assert s.doNotDestroy && s.elemSize == normCapacity;
                    long handle = s.allocate();
                    assert handle >= 0;
                    if (group.allocate(normCapacity, getMemoryMode())) {
                        s.chunk.initBufWithSubpage(byteBuf, handle, normCapacity);
                    }
                    if (byteBuf.allocateFailed()) {
                        group.free(normCapacity, getMemoryMode());
                    }
                    return;
                }
            }

            synchronized (this) {
                allocateBytebuf(byteBuf, normCapacity, group);
            }
            return;
        }

        if (normCapacity <= chunkSize) {
            synchronized (this) {
                allocateBytebuf(byteBuf, normCapacity, group);
            }
        } else {
            //todo allocate huge size
            LOGGER.warn(String.format("not support huge size:%d!", normCapacity));
        }
    }

    // Method must be called inside synchronized(this) { ... } block
    protected abstract void allocateBytebuf(ByteBuf<T> buf, int normCapacity, MemoryGroup group);

    void free(Chunk<T> chunk, long handle, long length) {
        freeChunk(chunk, handle, length);
    }

    void freeChunk(Chunk<T> chunk, long handle, long length) {
        final boolean needDestoryChunk;
        synchronized (this) {
            needDestoryChunk = !chunk.parent.free(chunk, handle);
        }
        if (needDestoryChunk) {
            // destroyChunk not need to be called while holding the synchronized lock.
            destroyChunk(chunk);
        }
        usedMemory.getAndAdd(-1 * length);
    }

    int normalizeCapacity(int reqCapacity) {
        if (reqCapacity < 0) {
            throw new IllegalArgumentException("capacity: " + reqCapacity + " (expected: 0+)");
        }

        if (reqCapacity >= chunkSize) {
            return reqCapacity;
        }

        if (reqCapacity <= ESegmentSize.smallest().size()) {
            return ESegmentSize.smallest().size();
        }

        int normalizedCapacity = reqCapacity;
        normalizedCapacity--;
        normalizedCapacity |= normalizedCapacity >>> 1;
        normalizedCapacity |= normalizedCapacity >>> 2;
        normalizedCapacity |= normalizedCapacity >>> 4;
        normalizedCapacity |= normalizedCapacity >>> 8;
        normalizedCapacity |= normalizedCapacity >>> 16;
        normalizedCapacity++;

        if (normalizedCapacity < 0) {
            normalizedCapacity >>>= 1;
        }

        return normalizedCapacity;
    }

    static ESegmentSize normalize(int noramlCapacity) {
        return ESegmentSize.valueOf(noramlCapacity);
    }

    abstract MemoryMode getMemoryMode();

    Page<T> getSuitablePageHead(int size) {
        int normCapacity = normalizeCapacity(size);
        return subpages.get(normalize(normCapacity));
    }

    protected void destroy() {
        subpages.forEach((k, v) -> v.destroy());
        destroyChunkList(qInit, q000, q025, q050, q075, q100);
    }

    private static void destroyChunkList(ChunkList... lists) {
        for (ChunkList chunkList : lists) {
            chunkList.destroy();
        }
    }

    abstract boolean canExpandCapacity();

    abstract void shrinkCapacity();

    abstract void destroyChunk(Chunk<T> chunk);

    protected abstract ByteBuf<T> newByteBuf(int size);

    protected abstract Chunk<T> newChunk(int pageSize, int maxOrder, int pageShifts, int chunkSize);

    protected abstract String dump();

    AtomicInteger groupThreads(MemoryGroup group) {
        if (!groupThreadCounter.containsKey(group)) {
            groupThreadCounter.put(group, new AtomicInteger(0));
        }
        return groupThreadCounter.get(group);
    }

    @Override
    public int numThreadCaches() {
        int num = 0;
        synchronized (groupThreadCounter) {
            for (Entry a : groupThreadCounter.entrySet()) {
                num += ((AtomicInteger) a.getValue()).intValue();
            }
        }
        return num;
    }

    @Override
    public long numAllocations() {
        return 0;
    }

    @Override
    public long allocateBytes() {
        return this.allocateMemory;
    }

    @Override
    public long numActiveBytes() {
        return usedMemory.get();
    }

    @Override
    public long freeBytes() {
        return allocateBytes() - numActiveBytes();
    }

    protected void reloadMemoryStatics(MemoryMode memoryMode) {
        if (memoryMode == MemoryMode.OFF_HEAP) {
            long totalOffHeapMemory = memoryManager.totalAllocateOffHeapMemory();
            if (totalOffHeapMemory > 0) {
                MemoryGroupManger.getInstance()
                    .resetMemory(totalOffHeapMemory, chunkSize, memoryMode);
            }
        }
    }

    protected void updateAllocateMemory() {
        this.allocateMemory = (long) this.currentChunkNum * chunkSize;
    }

    @Override
    public synchronized String toString() {
        String newLine = "\n";
        StringBuilder buf = new StringBuilder()
            .append("Chunk(s) at 0~25%:")
            .append(qInit)
            .append(newLine)
            .append("Chunk(s) at 0~50%:")
            .append(q000)
            .append(newLine)
            .append("Chunk(s) at 25~75%:")
            .append(q025)
            .append(newLine)
            .append("Chunk(s) at 50~100%:")
            .append(q050)
            .append(newLine)
            .append("Chunk(s) at 75~100%:")
            .append(q075)
            .append(newLine)
            .append("Chunk(s) at 100%:")
            .append(q100)
            .append(newLine)
            .append(dump())
            .append(newLine);

        return buf.toString();
    }

}
