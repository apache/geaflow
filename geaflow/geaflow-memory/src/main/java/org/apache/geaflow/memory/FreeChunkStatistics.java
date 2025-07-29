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

import com.google.common.collect.EvictingQueue;
import java.util.Queue;

public class FreeChunkStatistics {

    private Queue<Integer> timeQueue;
    private long lastTime;
    private int free = Integer.MAX_VALUE;
    private int queueSize;
    private int diffTime = 60 * 1000;

    public FreeChunkStatistics(int queueSize) {
        timeQueue = EvictingQueue.create(queueSize);
        this.queueSize = queueSize;
    }

    public FreeChunkStatistics(int queueSize, int diffTime) {
        this.queueSize = queueSize;
        this.diffTime = diffTime;
        timeQueue = EvictingQueue.create(queueSize);
    }

    public void update(int free) {
        long time = System.currentTimeMillis();

        if (lastTime == 0) {
            lastTime = time;
        }

        if (time - lastTime > diffTime) {
            timeQueue.add(this.free);
            lastTime = time;
            this.free = Integer.MAX_VALUE;
        }

        this.free = Math.min(free, this.free);
    }

    public int getMinFree() {
        int minFree = free;

        for (int free : timeQueue) {
            minFree = Math.min(minFree, free);
        }
        return minFree;
    }

    public boolean isFull() {
        return timeQueue.size() + 1 >= queueSize;
    }

    public void clear() {
        lastTime = 0;
        free = Integer.MAX_VALUE;
        timeQueue.clear();
    }

    @Override
    public String toString() {
        return "FreeChunkStatistics{" + "timeQueue=" + timeQueue + ", lastTime=" + lastTime
            + ", free=" + free + ", queueSize=" + queueSize + ", diffTime=" + diffTime + '}';
    }
}
