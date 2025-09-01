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
import com.google.common.collect.Maps;
import java.io.Serializable;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.geaflow.memory.metric.MemoryGroupMetric;

public final class MemoryGroup implements MemoryGroupMetric, Serializable {

    private final String name;
    private final AtomicInteger threads = new AtomicInteger(0);
    private final int spanSize;
    private final AtomicLong byteBufCount = new AtomicLong(0);
    private double ratio;
    private final Map<MemoryMode, Counter> counterMap;

    MemoryGroup(String name, int spanSize) {
        this.name = name;
        this.spanSize = spanSize;
        counterMap = Maps.newHashMap();
        counterMap.put(MemoryMode.OFF_HEAP, new OffHeapCounter());
        counterMap.put(MemoryMode.ON_HEAP, new OnHeapCounter());
    }

    public String getName() {
        return name;
    }

    public boolean allocate(long bytes, MemoryMode memoryMode) {
        return counterMap.get(memoryMode).allocate(bytes);
    }

    public boolean free(long bytes, MemoryMode memoryMode) {
        return counterMap.get(memoryMode).free(bytes);
    }

    public void updateByteBufCount(long count) {
        byteBufCount.addAndGet(count);
    }

    public void setSharedFreeBytes(AtomicLong sharedFreeBytes, MemoryMode memoryMode) {
        counterMap.get(memoryMode).setSharedFreeBytes(sharedFreeBytes);
    }

    @Override
    public long usedBytes() {
        return counterMap.values().stream().mapToLong(Counter::usedBytes).sum();
    }

    @Override
    public long usedOnHeapBytes() {
        return counterMap.get(MemoryMode.ON_HEAP).usedBytes();
    }

    @Override
    public long usedOffHeapBytes() {
        return counterMap.get(MemoryMode.OFF_HEAP).usedBytes();
    }

    @Override
    public long baseBytes() {
        return counterMap.values().stream().mapToLong(Counter::baseBytes).sum();
    }

    public void increaseThreads() {
        threads.incrementAndGet();
    }

    public void decreaseThreads() {
        threads.decrementAndGet();
    }

    public int getThreads() {
        return threads.get();
    }

    public int getSpanSize() {
        return spanSize;
    }

    public void setRatio(String decimalRatio) {
        int dr = "*".equals(decimalRatio) ? 0 : Integer.parseInt(decimalRatio);
        Preconditions.checkArgument(dr <= 100, "group ratio expect[0-100]");
        this.ratio = 0.01 * dr;
    }

    public double getRatio() {
        return ratio;
    }

    public void setBaseBytes(long totalMemory, int chunkSize, MemoryMode memoryMode) {
        long baseBytes = normalize((long) (totalMemory * ratio), chunkSize);
        counterMap.get(memoryMode).setBaseBytes(baseBytes);
    }

    private long normalize(long size, int chunkSize) {
        return (size & (long) (chunkSize - 1)) != 0
            ? ((size >>> (int) (Math.log(chunkSize) / Math.log(2.0))) + 1) * chunkSize : size;
    }

    @Override
    public double usage() {
        if (baseBytes() <= 0) {
            return 0.0;
        }
        return (double) usedBytes() / baseBytes();
    }

    @Override
    public long byteBufNum() {
        return byteBufCount.get();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        MemoryGroup that = (MemoryGroup) o;
        return this.name.equals(that.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();

        sb.append("[").append(name).append(",").append(baseBytes()).append(",").append(usedBytes())
            .append(",").append(usage()).append(",").append(byteBufNum()).append(",")
            .append(threads.get()).append("]");

        return sb.toString();
    }

    public void reset() {
        byteBufCount.set(0);
        threads.set(0);
        counterMap.forEach((k, v) -> v.reset());
    }

    interface Counter {

        boolean allocate(long bytes);

        boolean free(long bytes);

        long usedBytes();

        void setSharedFreeBytes(AtomicLong sharedFreeBytes);

        void setBaseBytes(long bytes);

        long baseBytes();

        void reset();
    }

    class OffHeapCounter implements Counter {

        private AtomicLong usedSharedBytes = new AtomicLong(0);
        private AtomicLong baseFreeBytes = new AtomicLong(0);
        private AtomicLong sharedFreeBytes;
        private long baseBytes;

        @Override
        public boolean allocate(long bytes) {
            if (baseBytes > 0) {
                if (baseFreeBytes.addAndGet(-1 * bytes) >= 0) {
                    return true;
                } else {
                    baseFreeBytes.getAndAdd(bytes);
                }
            }

            if (sharedFreeBytes.get() < bytes) {
                return false;
            }

            if (sharedFreeBytes.addAndGet(-1 * bytes) < 0) {
                sharedFreeBytes.getAndAdd(bytes);
                return false;
            }
            usedSharedBytes.getAndAdd(bytes);
            return true;
        }

        @Override
        public boolean free(long bytes) {
            if (baseBytes > 0) {
                if (baseFreeBytes.addAndGet(bytes) <= baseBytes) {
                    return true;
                } else {
                    baseFreeBytes.getAndAdd(-1 * bytes);
                }
            }

            sharedFreeBytes.getAndAdd(bytes);
            usedSharedBytes.getAndAdd(-1 * bytes);
            return true;
        }

        @Override
        public long usedBytes() {
            return usedSharedBytes.get() + baseBytes - baseFreeBytes.get();
        }

        @Override
        public void setSharedFreeBytes(AtomicLong sharedFreeBytes) {
            this.sharedFreeBytes = sharedFreeBytes;
        }

        @Override
        public void setBaseBytes(long bytes) {
            long diff = bytes - this.baseBytes;
            this.baseBytes = bytes;
            this.baseFreeBytes.getAndAdd(diff);
        }

        @Override
        public long baseBytes() {
            return baseBytes;
        }

        @Override
        public void reset() {
            usedSharedBytes.set(0);
            baseFreeBytes.set(0);
            baseBytes = 0;
        }
    }

    static class OnHeapCounter implements Counter {

        private AtomicLong usedBytes = new AtomicLong(0);

        @Override
        public boolean allocate(long bytes) {
            usedBytes.getAndAdd(bytes);
            return true;
        }

        @Override
        public boolean free(long bytes) {
            usedBytes.getAndAdd(-1 * bytes);
            return true;
        }

        @Override
        public long usedBytes() {
            return usedBytes.get();
        }

        @Override
        public void setSharedFreeBytes(AtomicLong sharedFreeBytes) {

        }

        @Override
        public void setBaseBytes(long bytes) {

        }

        @Override
        public long baseBytes() {
            return 0;
        }

        @Override
        public void reset() {
            usedBytes.set(0);
        }
    }

}
