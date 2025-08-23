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

package org.apache.geaflow.shuffle.pipeline.buffer;

import static org.apache.geaflow.common.config.keys.ExecutionConfigKeys.CONTAINER_HEAP_SIZE_MB;
import static org.apache.geaflow.common.config.keys.ExecutionConfigKeys.SHUFFLE_HEAP_MEMORY_FRACTION;
import static org.apache.geaflow.common.config.keys.ExecutionConfigKeys.SHUFFLE_MEMORY_POOL_ENABLE;
import static org.apache.geaflow.common.config.keys.ExecutionConfigKeys.SHUFFLE_MEMORY_SAFETY_FRACTION;
import static org.apache.geaflow.common.config.keys.ExecutionConfigKeys.SHUFFLE_OFFHEAP_MEMORY_FRACTION;

import java.util.concurrent.atomic.AtomicLong;
import org.apache.commons.io.FileUtils;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.memory.MemoryManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ShuffleMemoryTracker {

    private static final Logger LOGGER = LoggerFactory.getLogger(ShuffleMemoryTracker.class);

    private final long maxShuffleSize;
    private final AtomicLong usedMemory;
    private MemoryManager memoryPoolManager;

    public ShuffleMemoryTracker(Configuration config) {
        boolean memoryPool = config.getBoolean(SHUFFLE_MEMORY_POOL_ENABLE);
        double safetyFraction = config.getDouble(SHUFFLE_MEMORY_SAFETY_FRACTION);

        long maxMemorySize;
        if (memoryPool) {
            memoryPoolManager = MemoryManager.build(config);
            maxMemorySize = (long) (memoryPoolManager.maxMemory() * safetyFraction);
            double fraction = config.getDouble(SHUFFLE_OFFHEAP_MEMORY_FRACTION);
            maxShuffleSize = (long) (maxMemorySize * fraction);
        } else {
            long maxHeapSize = config.getInteger(CONTAINER_HEAP_SIZE_MB) * FileUtils.ONE_MB;
            maxMemorySize = (long) (maxHeapSize * safetyFraction);
            double fraction = config.getDouble(SHUFFLE_HEAP_MEMORY_FRACTION);
            maxShuffleSize = (long) (maxMemorySize * fraction);
        }

        usedMemory = new AtomicLong(0);
        LOGGER.info("memoryPool:{} maxMemory:{}mb shuffleMax:{}mb", memoryPool,
            maxMemorySize / FileUtils.ONE_MB, maxShuffleSize / FileUtils.ONE_MB);
    }

    public boolean requireMemory(long requiredBytes) {
        if (usedMemory.get() < 0) {
            LOGGER.warn("memory statistic incorrect!");
        }
        if (requiredBytes < 0) {
            throw new IllegalArgumentException("invalid required bytes:" + requiredBytes);
        } else if (requiredBytes == 0) {
            return maxShuffleSize >= usedMemory.get();
        } else {
            return maxShuffleSize >= usedMemory.addAndGet(requiredBytes);
        }
    }

    public boolean checkMemoryEnough() {
        return usedMemory.get() < maxShuffleSize;
    }

    public long releaseMemory(long releasedBytes) {
        return usedMemory.addAndGet(releasedBytes * -1);
    }

    public long getUsedMemory() {
        return usedMemory.get();
    }

    public double getUsedRatio() {
        return usedMemory.get() * 1.0 / maxShuffleSize;
    }

    public void release() {
        if (memoryPoolManager != null) {
            memoryPoolManager.dispose();
        }
    }

}
