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
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.geaflow.memory.metric.ChunkListMetric;
import org.apache.geaflow.memory.metric.ChunkMetric;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ChunkList<T> implements ChunkListMetric {

    private static final Logger LOGGER = LoggerFactory.getLogger(ChunkList.class);

    private static final Iterator<ChunkMetric> EMPTY_METRICS = Collections.<ChunkMetric>emptyList().iterator();

    private final AbstractMemoryPool<T> pool;

    private ChunkList<T> nextList;
    private ChunkList<T> preList;

    private Chunk<T> head;

    private final int minUsage;
    private final int maxUsage;
    AtomicInteger freeChunk;

    public ChunkList(AbstractMemoryPool<T> pool, int minUsage, int maxUsage) {
        this.pool = pool;
        this.minUsage = minUsage;
        this.maxUsage = maxUsage;
        freeChunk = new AtomicInteger(0);
    }

    public void setNextList(ChunkList<T> nextList) {
        this.nextList = nextList;
    }

    public void setPreList(ChunkList<T> preList) {
        this.preList = preList;
    }

    boolean allocate(ByteBuf<T> buf, int capacity) {

        try {
            if (head == null) {
                return false;
            }

            for (Chunk<T> chunk = head; ; ) {
                if (chunk.isFree()) {
                    freeChunk.addAndGet(-1);
                }
                long handle = chunk.allocate(capacity);
                if (handle < 0) {
                    chunk = chunk.next;
                    if (chunk == null) {
                        return false;
                    }
                } else {
                    chunk.initBuf(buf, handle, capacity);
                    if (chunk.usage() >= maxUsage()) {
                        remove(chunk);
                        nextList.add(chunk);
                    }
                    return true;
                }
            }
        } catch (Throwable t) {
            LOGGER.error(String.format("max=%d,min=%d allocate failed!", maxUsage, minUsage), t);
            throw t;
        }
    }

    boolean free(Chunk<T> chunk, long handle) {
        chunk.free(handle);
        if (chunk.usage() < minUsage) {
            remove(chunk);
            return internalMove(chunk);
        }
        return true;
    }

    private boolean move(Chunk<T> chunk) {
        assert chunk.usage() < maxUsage;

        if (chunk.usage() < minUsage) {
            return internalMove(chunk);
        }

        internalAdd(chunk);
        return true;
    }

    void add(Chunk<T> chunk) {
        if (chunk.usage() >= maxUsage) {
            nextList.add(chunk);
            return;
        }
        internalAdd(chunk);
    }

    void internalAdd(Chunk<T> chunk) {
        chunk.parent = this;
        if (head == null) {
            head = chunk;
            chunk.prev = null;
            chunk.next = null;
        } else {
            chunk.prev = null;
            chunk.next = head;
            head.prev = chunk;
            head = chunk;
        }
        if (chunk.isFree()) {
            freeChunk.addAndGet(1);
        }
    }

    private boolean internalMove(Chunk<T> chunk) {
        if (preList == null) {
            // 堆内内存直接释放
            if (pool.getMemoryMode() == MemoryMode.ON_HEAP) {
                Preconditions.checkArgument(chunk.usage() == 0);
                return false;
            } else {
                // 堆外先放回，等待触发缩容
                internalAdd(chunk);
                //缩容入口
                pool.shrinkCapacity();

                return true;
            }
        }

        return preList.move(chunk);
    }

    void remove(Chunk<T> cur) {
        if (cur == head) {
            head = cur.next;
            if (head != null) {
                head.prev = null;
            }
        } else {
            Chunk<T> next = cur.next;
            cur.prev.next = next;
            if (next != null) {
                next.prev = cur.prev;
            }
        }
    }

    @Override
    public int minUsage() {
        return Math.max(1, this.minUsage);
    }

    @Override
    public int maxUsage() {
        return Math.min(this.maxUsage, 100);
    }

    @Override
    public Iterator<ChunkMetric> iterator() {
        synchronized (pool) {
            if (head == null) {
                return EMPTY_METRICS;
            }
            List<ChunkMetric> metrics = new ArrayList<>();
            for (Chunk<T> cur = head; ; ) {
                metrics.add(cur);
                cur = cur.next;
                if (cur == null) {
                    break;
                }
            }
            return metrics.iterator();
        }
    }

    void destroy() {
        Chunk<T> cur = head;
        while (cur != null) {
            pool.destroyChunk(cur);
            cur = cur.next;
        }
        head = null;
    }

    int freeChunkNum() {
        return freeChunk.get();
    }

    Chunk<T> getHead() {
        return head;
    }

    @Override
    public String toString() {
        StringBuilder buf = new StringBuilder();
        synchronized (pool) {
            if (head == null) {
                return "[none]";
            }
            buf.append("[free: ").append(freeChunkNum());
            int size = 0;
            for (Chunk<T> cur = head; ; ) {
                size++;
                cur = cur.next;
                if (cur == null) {
                    break;
                }
            }
            buf.append(", total: ").append(size).append("]");
        }
        return buf.toString();
    }
}
