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
import com.google.common.collect.Sets;
import java.util.List;
import java.util.Set;
import org.apache.geaflow.common.utils.MemoryUtils;
import org.apache.geaflow.memory.exception.GeaflowOutOfMemoryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class ThreadLocalCache {
    private static final Logger LOGGER = LoggerFactory.getLogger(ThreadLocalCache.class);

    // local pool which binds to the ThreadLocalCache is the first priority to allocate memory.
    private AbstractMemoryPool localPool;

    // temp pool which route to a suitable pool when local pool is full.
    private AbstractMemoryPool tempPool;

    // try to allocate memory form adaptive pool when all pools are full.
    private AbstractMemoryPool adaptivePool;

    private final AbstractMemoryPool[] pools;

    private MemoryGroup memoryGroup;

    public ThreadLocalCache(AbstractMemoryPool[] pools, AbstractMemoryPool localPool,
                            AbstractMemoryPool adaptivePool, MemoryGroup memoryGroup) {

        this.pools = pools;

        this.localPool = localPool;
        this.adaptivePool = adaptivePool;

        this.memoryGroup = memoryGroup;
        this.memoryGroup.increaseThreads();
    }

    private int suitablePoolIndex(int size, Set<Integer> visited) {
        if (pools == null || pools.length == 0) {
            return -1;
        }

        int index = -1;
        AbstractMemoryPool minPool = null;
        for (int i = 0; i < pools.length; i++) {
            if (visited.contains(i)) {
                continue;
            }
            AbstractMemoryPool pool = pools[i];
            if (pool.freeBytes() < size && !pool.canExpandCapacity()) {
                visited.add(i);
                continue;
            }
            if (minPool == null || pool.groupThreads(memoryGroup).intValue() < minPool.groupThreads(memoryGroup).intValue()) {
                minPool = pool;
                index = i;
            }
        }

        return index;
    }

    ByteBuf requireBuf(int size) {

        if (this.localPool.getMemoryMode() == MemoryMode.ON_HEAP) {
            return this.localPool.oneAllocate(size, memoryGroup);
        }
        ByteBuf byteBuf = null;
        if (canAllocate(this.localPool, size)) {
            byteBuf = this.localPool.oneAllocate(size, memoryGroup);
        }

        if (oneAllocateFailed(byteBuf) && this.tempPool != null && canAllocate(this.tempPool, size)) {
            byteBuf = this.tempPool.oneAllocate(size, memoryGroup);
        }

        Set<Integer> visited = Sets.newHashSet();
        while (byteBuf == null || byteBuf.allocateFailed()) {

            int i = suitablePoolIndex(size, visited);
            if (i == -1) {
                break;
            }
            byteBuf = pools[i].oneAllocate(size, memoryGroup);
            visited.add(i);

            if (!byteBuf.allocateFailed()) {
                updateTempPool(pools[i]);
                break;
            }

            if (visited.size() == pools.length) {
                updateTempPool(null);
                break;
            }
        }

        byteBuf = tryOneAllocate(byteBuf, size);
        if (oneAllocateFailed(byteBuf)) {
            reportOutOfMemoryException();
            throw new GeaflowOutOfMemoryException("out of memory");
        }

        return byteBuf;
    }

    private boolean oneAllocateFailed(ByteBuf byteBuf) {
        return (byteBuf == null || byteBuf.allocateFailed());
    }

    private ByteBuf tryOneAllocate(ByteBuf buf, int size) {
        if (adaptivePool != null && oneAllocateFailed(buf)) {
            buf = adaptivePool.oneAllocate(size, memoryGroup);
        }
        return buf;
    }

    private void updateTempPool(AbstractMemoryPool newPool) {
        if (this.tempPool != null) {
            this.tempPool.groupThreads(memoryGroup).decrementAndGet();
        }

        this.tempPool = newPool;

        if (this.tempPool != null) {
            this.tempPool.groupThreads(memoryGroup).incrementAndGet();
        }
    }

    List<ByteBuf> requireBufs(int size) {

        if (this.localPool.getMemoryMode() == MemoryMode.ON_HEAP) {
            return this.localPool.allocate(size, memoryGroup);
        }

        List<ByteBuf> byteBufs = Lists.newArrayList();
        if (canAllocate(this.localPool, size)) {
            byteBufs.addAll(this.localPool.allocate(size, memoryGroup));
        }

        int remain = allocateFailedSize(byteBufs, size);

        if (remain > 0 && this.tempPool != null && canAllocate(this.tempPool, remain)) {
            byteBufs.addAll(this.tempPool.allocate(remain, memoryGroup));
            remain = allocateFailedSize(byteBufs, size);
        }

        Set<Integer> visited = Sets.newHashSet();
        while (remain > 0) {
            int i = suitablePoolIndex(remain > this.localPool.chunkSize ? this.localPool.chunkSize : remain, visited);
            if (i == -1) {
                break;
            }
            byteBufs.addAll(pools[i].allocate(remain, memoryGroup));
            visited.add(i);

            remain = allocateFailedSize(byteBufs, size);

            if (remain <= 0) {
                updateTempPool(pools[i]);
                break;
            }

            if (visited.size() == pools.length) {
                updateTempPool(null);
                break;
            }
        }

        // 如果部分失败，则尝试从adaptivePool继续申请剩余部分内存
        if (remain > 0 && adaptivePool != null) {
            byteBufs.addAll(adaptivePool.allocate(remain, memoryGroup));
            remain = allocateFailedSize(byteBufs, size);
        }

        if (remain > 0) {
            reportOutOfMemoryException();
            throw new GeaflowOutOfMemoryException("out of memory");
        }

        return byteBufs;
    }

    private boolean canAllocate(AbstractMemoryPool pool, int size) {
        return pool.freeBytes() >= size || pool.canExpandCapacity();
    }

    private int allocateFailedSize(List<ByteBuf> byteBufs, int totalSize) {
        int size = 0;
        for (ByteBuf buf : byteBufs) {
            size += buf.getLength();
        }
        return totalSize - size;
    }

    public AbstractMemoryPool getLocalPool() {
        return this.localPool;
    }

    /**
     * Should be called if the Thread that uses this cache is about to exist to
     * release resources out of the cache.
     */
    void free() {
        if (localPool != null) {
            localPool.groupThreads(memoryGroup).decrementAndGet();
        }
        memoryGroup.decreaseThreads();
    }

    protected void reportOutOfMemoryException() {
        try {
            MemoryManager memoryManager = MemoryManager.getInstance();
            String msg = String.format("Direct memory used %s, max direct memory %s. Please "
                    + "reduce -Xmx or set -XX:MaxDirectMemorySize to enlarge direct memory.",
                MemoryUtils.humanReadableByteCount(memoryManager.usedMemory()),
                MemoryUtils.humanReadableByteCount(memoryManager.maxMemory()));
            LOGGER.warn("{}", msg);

        } catch (Throwable t) {
            LOGGER.error("report exception failed", t);
        }
    }

}
