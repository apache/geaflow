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

import java.nio.ByteBuffer;
import org.apache.geaflow.memory.config.MemoryConfigKeys;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DirectMemoryPool extends AbstractMemoryPool<ByteBuffer> {

    private static final Logger LOGGER = LoggerFactory.getLogger(DirectMemoryPool.class);

    private final int maxChunkNum;
    private final int initChunkNum;
    private FreeChunkStatistics statistics;
    private int count = 0;
    private static final int MAX_COUNT = 10;
    private static final double SHRINK_RATIO = 0.2d;
    // enlarge the memory by 8 * chunkSize
    private static final int EXPANSION_STEP_SIZE = 8;

    public DirectMemoryPool(MemoryManager memoryManager, int pageSize, int maxOrder, int pageShifts,
                            int chunkSize, int initChunkNum, int maxChunkNum) {
        super(memoryManager, pageSize, maxOrder, pageShifts, chunkSize);

        for (int i = 0; i < initChunkNum; i++) {
            qInit.add(newChunk(pageSize, maxOrder, pageShifts, chunkSize));
        }
        this.currentChunkNum = initChunkNum;
        this.maxChunkNum = maxChunkNum;
        this.initChunkNum = initChunkNum;
        statistics = new FreeChunkStatistics(
            memoryManager.config.getInteger(MemoryConfigKeys.MEMORY_TRIM_GAP_MINUTE));
        updateAllocateMemory();
    }

    @Override
    MemoryMode getMemoryMode() {
        return MemoryMode.OFF_HEAP;
    }

    @Override
    void shrinkCapacity() {

        if (this.currentChunkNum == this.initChunkNum || !statistics.isFull()) {
            return;
        }
        int minFreeChunk = statistics.getMinFree();
        if (((1.0 * minFreeChunk) / this.currentChunkNum) < SHRINK_RATIO) {
            return;
        }
        int needRemove = Math.min(minFreeChunk, this.currentChunkNum - this.initChunkNum);
        int removed = 0;
        synchronized (this) {
            removed = removeChunk(qInit, needRemove);
            if (removed < needRemove) {
                removed += removeChunk(q000, needRemove - removed);
            }
        }
        LOGGER.info("direct memory shrink capacity, need remove:{}, removed:{}, current "
            + "chunkNum:{}, max chunkNum:{} ", needRemove, removed, currentChunkNum, maxChunkNum);
        updateAllocateMemory();
        reloadMemoryStatics(getMemoryMode());
        statistics.clear();
    }

    private int removeChunk(ChunkList chunkList, int needRemove) {
        Chunk<ByteBuffer> chunk = chunkList.getHead();
        if (chunk == null) {
            return 0;
        }
        int removed = 0;
        for (; ; ) {
            if (chunk.isFree()) {
                chunkList.remove(chunk);
                chunkList.freeChunk.getAndAdd(-1);
                chunk.destroy();
                currentChunkNum--;
                removed++;
            }
            if (removed == needRemove) {
                break;
            }
            chunk = chunk.next;
            if (chunk == null) {
                break;
            }
        }
        return removed;
    }

    @Override
    protected void allocateBytebuf(ByteBuf<ByteBuffer> buf, int normCapacity, MemoryGroup group) {

        if (group.allocate(normCapacity, getMemoryMode())) {
            if (q050.allocate(buf, normCapacity) || q025.allocate(buf, normCapacity)
                || q000.allocate(buf, normCapacity) || qInit.allocate(buf, normCapacity)
                || q075.allocate(buf, normCapacity)) {
                if (++count % MAX_COUNT == 0) {
                    statistics.update(qInit.freeChunkNum() + q000.freeChunkNum());
                    count = 0;
                }
                return;
            } else {
                group.free(normCapacity, getMemoryMode());
            }
        }

        if (!canExpandCapacity()) {
            return;
        }

        int expansionSize = getExpansionSize();

        for (int i = 0; i < expansionSize; i++) {
            qInit.add(newChunk(pageSize, maxOrder, pageShifts, chunkSize));
        }

        this.currentChunkNum += expansionSize;
        statistics.clear();
        updateAllocateMemory();
        reloadMemoryStatics(getMemoryMode());

        boolean allocateSuccess = false;

        if (group.allocate(normCapacity, getMemoryMode())) {
            allocateSuccess = qInit.allocate(buf, normCapacity);
        }
        if (!allocateSuccess) {
            group.free(normCapacity, getMemoryMode());
        }

        LOGGER.info("direct memory expand capacity, expand chunkNum:{}, current chunkNum:{}, max "
            + "chunkNum:{}", expansionSize, currentChunkNum, maxChunkNum);
    }

    @Override
    boolean canExpandCapacity() {
        return this.maxChunkNum > this.currentChunkNum;
    }

    private int getExpansionSize() {
        int size = (maxChunkNum - initChunkNum) / EXPANSION_STEP_SIZE;
        if (currentChunkNum + size > maxChunkNum) {
            size = maxChunkNum - currentChunkNum;
        }
        return size > 0 ? size : 1;
    }

    @Override
    void destroyChunk(Chunk<ByteBuffer> chunk) {

        if (DirectMemory.useDirectBufferNoCleaner()) {
            DirectMemory.freeDirectNoCleaner(chunk.memory);
        } else {
            DirectMemory.freeDirectBuffer(chunk.memory);
        }
    }


    @Override
    protected ByteBuf<ByteBuffer> newByteBuf(int size) {
        return new ByteBuf<ByteBuffer>();
    }

    @Override
    protected Chunk<ByteBuffer> newChunk(int pageSize, int maxOrder, int pageShifts,
                                         int chunkSize) {
        ByteBuffer buffer = allocateDirect(chunkSize);
        return new Chunk<>(this, buffer, pageSize, maxOrder, pageShifts, chunkSize, 0);
    }

    private static ByteBuffer allocateDirect(int capacity) {
        return DirectMemory.useDirectBufferNoCleaner() ? DirectMemory.allocateDirectNoCleaner(
            capacity) : ByteBuffer.allocateDirect(capacity);
    }

    void setStatistics(FreeChunkStatistics statistics) {
        this.statistics = statistics;
    }


    protected String dump() {
        return String.format("direct memory capacity statistics, full:%b, minFreeChunk:%d, "
                + "currentChunkNum:%d, statistic:%s", statistics.isFull(), statistics.getMinFree(),
            this.currentChunkNum, statistics.toString());
    }
}
