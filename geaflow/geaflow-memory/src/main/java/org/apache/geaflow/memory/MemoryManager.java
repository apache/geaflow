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

import com.google.common.base.Preconditions;
import java.io.Serializable;
import java.util.List;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.common.utils.MemoryUtils;
import org.apache.geaflow.memory.config.MemoryConfigKeys;
import org.apache.geaflow.memory.thread.BaseMemoryGroupThreadLocal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class MemoryManager implements Serializable {

    private static final Logger LOGGER = LoggerFactory.getLogger(MemoryManager.class);

    public static final int MIN_PAGE_SIZE = 4 * 1024;
    private static final int MAX_CHUNK_SIZE = (int) (((long) Integer.MAX_VALUE + 1) / 2);

    private AbstractMemoryPool[] pools;

    private boolean autoAdaptEnable;
    private AbstractMemoryPool[] adaptivePools;

    private static MemoryManager memoryManager;
    private final BaseMemoryGroupThreadLocal<ThreadLocalCache> threadLocal;
    private final int chunkSize;
    private final long maxMemory;

    protected Configuration config;

    public static synchronized MemoryManager build(Configuration config) {
        if (memoryManager == null) {
            memoryManager = new MemoryManager(config);
        }
        return memoryManager;
    }

    public static MemoryManager getInstance() {
        return memoryManager;
    }

    private MemoryManager(Configuration config) {

        this.config = config;

        this.autoAdaptEnable = config.getBoolean(MemoryConfigKeys.MEMORY_AUTO_ADAPT_ENABLE);

        long offHeapSize = config.getLong(MemoryConfigKeys.OFF_HEAP_MEMORY_SIZE_MB) * MemoryUtils.MB;
        long heapSize = config.getLong(MemoryConfigKeys.ON_HEAP_MEMORY_SIZE_MB) * MemoryUtils.MB;

        int pageSize = config.getInteger(MemoryConfigKeys.MEMORY_PAGE_SIZE);
        int maxOrder = config.getInteger(MemoryConfigKeys.MEMORY_MAX_ORDER);
        int pageShifts = validateAndCalculatePageShifts(pageSize);

        if (offHeapSize == 0 && heapSize == 0) {
            offHeapSize = (long) (MemoryConfigKeys.JVM_MAX_DIRECT_MEMORY * 0.3);
        }

        chunkSize = validateAndCalculateChunkSize(pageSize, maxOrder);
        int poolSize = config.getInteger(MemoryConfigKeys.MEMORY_POOL_SIZE);
        int initChunkNum = 0;
        int maxChunkNum = 0;
        long maxOffHeapSize = config.getLong(MemoryConfigKeys.MAX_DIRECT_MEMORY_SIZE);

        Preconditions.checkArgument(offHeapSize <= maxOffHeapSize,
            "OffHeapSize:%s is greater than maxOffHeapSize:%s", offHeapSize, maxOffHeapSize);

        if (offHeapSize > 0) {
            poolSize = poolSize > 0 ? poolSize : caculatePoolSize(offHeapSize, pageSize, maxOrder);
            pools = new AbstractMemoryPool[poolSize];
            initChunkNum = caculateChunkNum(poolSize, chunkSize, offHeapSize);
            maxChunkNum = caculateChunkNum(poolSize, chunkSize, maxOffHeapSize);
            maxChunkNum = Math.max(maxChunkNum, initChunkNum);

            for (int i = 0; i < poolSize; i++) {
                pools[i] = new DirectMemoryPool(this, pageSize, maxOrder, pageShifts, chunkSize,
                    initChunkNum, maxChunkNum);
            }

            if (autoAdaptEnable) {
                adaptivePools = new AbstractMemoryPool[poolSize];
                for (int i = 0; i < poolSize; i++) {
                    adaptivePools[i] = new HeapMemoryPool(this, pageSize, maxOrder, pageShifts, chunkSize);
                }
            }

            MemoryGroupManger.getInstance().load(config);
            MemoryGroupManger.getInstance().resetMemory((long) poolSize * initChunkNum * chunkSize,
                chunkSize, MemoryMode.OFF_HEAP);
        }

        if (heapSize > 0) {
            poolSize = poolSize > 0 ? poolSize : caculatePoolSize(heapSize, pageSize, maxOrder);
            pools = new AbstractMemoryPool[poolSize];
            for (int i = 0; i < poolSize; i++) {
                pools[i] = new HeapMemoryPool(this, pageSize, maxOrder, pageShifts, chunkSize);
            }
        }

        this.maxMemory = (long) chunkSize * poolSize * maxChunkNum;

        LOGGER.info("MemoryManager init, offHeapSize:{},maxOffHeapSize:{},heapSize:{},"
                + "pageSize:{},maxOrder:{},pageShifts:{},chunkSize:{},poolSize:{},initChunkNum:{},"
                + "maxChunkNum:{}", offHeapSize, this.maxMemory, heapSize, pageSize, maxOrder,
            pageShifts, chunkSize, poolSize, initChunkNum, maxChunkNum);

        threadLocal = new BaseMemoryGroupThreadLocal<ThreadLocalCache>() {
            @Override
            protected synchronized ThreadLocalCache initialValue(MemoryGroup group) {
                int index = leastUsedPool(pools, group);
                AbstractMemoryPool localPool = pools[index];
                // get one adaptive pool for the local pool
                AbstractMemoryPool adaptivePool =
                    (autoAdaptEnable && adaptivePools != null) ? adaptivePools[index] : null;
                return new ThreadLocalCache(pools, localPool, adaptivePool, group);
            }

            @Override
            protected void notifyRemove(ThreadLocalCache threadLocalCache) {
                threadLocalCache.free();
            }
        };
    }

    private <T> int leastUsedPool(AbstractMemoryPool<T>[] pools, MemoryGroup group) {
        if (pools == null || pools.length == 0) {
            throw new IllegalArgumentException("pool size is empty");
        }

        int index = 0;
        for (int i = 1; i < pools.length; i++) {
            AbstractMemoryPool<T> pool = pools[i];
            if (pool.groupThreads(group).intValue() < pools[index].groupThreads(group).intValue()) {
                index = i;
            }
        }
        return index;
    }

    private static int validateAndCalculateChunkSize(int pageSize, int maxOrder) {
        if (maxOrder > 14) {
            throw new IllegalArgumentException("maxOrder: " + maxOrder + " (expected: 0-14)");
        }

        // Ensure the resulting chunkSize does not overflow.
        int chunkSize = pageSize;
        for (int i = maxOrder; i > 0; i--) {
            if (chunkSize > MAX_CHUNK_SIZE / 2) {
                throw new IllegalArgumentException(String.format(
                    "pageSize (%d) << maxOrder (%d) must not exceed %d", pageSize, maxOrder, MAX_CHUNK_SIZE));
            }
            chunkSize <<= 1;
        }
        return chunkSize;
    }

    static int validateAndCalculatePageShifts(int pageSize) {
        if (pageSize < MIN_PAGE_SIZE) {
            throw new IllegalArgumentException("pageSize: " + pageSize + " (expected: " + MIN_PAGE_SIZE + ")");
        }

        if ((pageSize & pageSize - 1) != 0) {
            throw new IllegalArgumentException("pageSize: " + pageSize + " (expected: power of 2)");
        }

        // Logarithm base 2. At this point we know that pageSize is a power of two.
        return Integer.SIZE - 1 - Integer.numberOfLeadingZeros(pageSize);
    }

    private int caculatePoolSize(long heapSize, int pageSize, int maxOrder) {
        int poolSize = (int) (heapSize / (pageSize << maxOrder) / 16);
        return poolSize > 0 ? poolSize : 1;
    }

    private int caculateChunkNum(int poolSize, int chunkSize, long initSize) {
        int chunkNum = (int) (initSize / poolSize / chunkSize);
        return chunkNum > 0 ? chunkNum : 1;
    }

    ThreadLocalCache localCache(MemoryGroup group) {
        ThreadLocalCache localCache = threadLocal.get(group);
        assert localCache != null;
        return localCache;
    }

    void removeCache() {
        BaseMemoryGroupThreadLocal.removeAll();
    }

    public MemoryView requireMemory(int size, MemoryGroup group) {
        return new MemoryView(requireBufs(size, group), group);
    }

    ByteBuf requireBuf(int size, MemoryGroup group) {
        ThreadLocalCache localCache = localCache(group);
        ByteBuf byteBuf = localCache.requireBuf(size);
        return byteBuf;
    }

    List<ByteBuf> requireBufs(int size, MemoryGroup group) {
        ThreadLocalCache localCache = localCache(group);
        List<ByteBuf> bufs = localCache.requireBufs(size);
        group.updateByteBufCount(bufs.size());
        return bufs;
    }

    public void dispose() {
        try {
            memoryManager.removeCache();
            for (int i = 0; i < pools.length; i++) {
                pools[i].destroy();
                pools[i] = null;
            }
            memoryManager = null;
            MemoryGroupManger.getInstance().clear();
            System.gc();
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }

    public String dumpStats() {

        StringBuilder buf = new StringBuilder(512);
        buf.append(this.getClass().getSimpleName())
            .append("(usedMemory: ").append(usedMemory())
            .append("; allocatedMemory: ").append(totalAllocateMemory())
            .append("; numPools: ").append(poolSize())
            .append("; numThreadLocalCaches: ").append(numThreadLocalCaches())
            .append("; chunkSize: ").append(chunkSize()).append(')').append("\n");

        int len = pools == null ? 0 : pools.length;
        if (len > 0) {
            boolean heap = pools[0].getMemoryMode() == MemoryMode.ON_HEAP;
            buf.append(len);
            if (heap) {
                buf.append(" heap pool(s):")
                    .append("\n");
            } else {
                buf.append(" direct pool(s):")
                    .append("\n");
            }

            for (AbstractMemoryPool a : pools) {
                buf.append(a);
            }
        } else {
            buf.append(" none pool!");
        }

        return buf.toString();
    }

    public long totalAllocateMemory() {
        return totalAllocateMemory(pools) + totalAllocateMemory(adaptivePools);
    }

    public long usedMemory() {
        return usedMemory(pools) + usedMemory(adaptivePools);
    }

    public long maxMemory() {
        return this.maxMemory;
    }

    public long totalAllocateHeapMemory() {
        if (pools == null) {
            return 0;
        }

        if (pools[0].getMemoryMode() == MemoryMode.ON_HEAP) {
            return totalAllocateMemory(pools);
        }
        return totalAllocateMemory(adaptivePools);
    }

    public long totalAllocateOffHeapMemory() {
        if (pools == null) {
            return 0;
        }

        if (pools.length > 0 && pools[0].getMemoryMode() == MemoryMode.OFF_HEAP) {
            return totalAllocateMemory(pools);
        }
        return 0;
    }

    public long usedHeapMemory() {
        if (pools == null) {
            return 0;
        }

        if (pools.length > 0 && pools[0].getMemoryMode() == MemoryMode.ON_HEAP) {
            return usedMemory(pools);
        }
        return usedMemory(adaptivePools);
    }

    public long usedOffHeapMemory() {
        if (pools == null) {
            return 0;
        }

        if (pools.length > 0 && pools[0].getMemoryMode() == MemoryMode.OFF_HEAP) {
            return usedMemory(pools);
        }
        return 0;
    }

    private static long usedMemory(AbstractMemoryPool<?>... pools) {
        if (pools == null) {
            return 0;
        }
        long used = 0;
        for (AbstractMemoryPool<?> arena : pools) {
            used += arena.numActiveBytes();
            if (used < 0) {
                return Long.MAX_VALUE;
            }
        }
        return used;
    }

    private static long totalAllocateMemory(AbstractMemoryPool<?>... pools) {
        if (pools == null) {
            return 0;
        }
        long used = 0;
        for (AbstractMemoryPool<?> arena : pools) {
            used += arena.allocateBytes();
            if (used < 0) {
                return Long.MAX_VALUE;
            }
        }
        return used;
    }

    public int numThreadLocalCaches() {
        AbstractMemoryPool<?>[] pools = this.pools;
        if (pools == null) {
            return 0;
        }

        int total = 0;
        for (AbstractMemoryPool<?> pool : pools) {
            total += pool.numThreadCaches();
        }

        return total;
    }

    public int poolSize() {
        return pools == null ? 0 : pools.length;
    }

    public int chunkSize() {
        return chunkSize;
    }

    @Override
    public String toString() {
        return dumpStats();
    }
}
