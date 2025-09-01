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
import com.google.common.collect.Lists;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.common.utils.MemoryUtils;
import org.apache.geaflow.memory.config.MemoryConfigKeys;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class MemoryGroupManger {

    private static final Logger LOGGER = LoggerFactory.getLogger(MemoryGroupManger.class);

    public static MemoryGroup DEFAULT = new MemoryGroup("default", -1);

    public static MemoryGroup SHUFFLE = new MemoryGroup("shuffle", (int) (16 * MemoryUtils.KB));

    public static MemoryGroup STATE = new MemoryGroup("state", -1);

    private static final Map<String, MemoryGroup> GROUPS = new LinkedHashMap<>();
    private static final AtomicLong SHARED_OFF_HEAP_FREE_BYTES = new AtomicLong(0);
    private static long totalOffHeapSharedMemory;

    private static MemoryGroupManger manger;

    public static synchronized MemoryGroupManger getInstance() {
        if (manger == null) {
            manger = new MemoryGroupManger();

            DEFAULT.setSharedFreeBytes(SHARED_OFF_HEAP_FREE_BYTES, MemoryMode.OFF_HEAP);
            SHUFFLE.setSharedFreeBytes(SHARED_OFF_HEAP_FREE_BYTES, MemoryMode.OFF_HEAP);
            STATE.setSharedFreeBytes(SHARED_OFF_HEAP_FREE_BYTES, MemoryMode.OFF_HEAP);

            // 顺序和ratio一致
            register(SHUFFLE);
            register(STATE);
            register(DEFAULT);
        }
        return manger;
    }

    void load(Configuration config) {
        String groupRatios = config.getString(MemoryConfigKeys.MEMORY_GROUP_RATIO);
        String[] ratios = groupRatios.split(":");
        Preconditions.checkArgument(ratios.length == GROUPS.size(),
            MemoryConfigKeys.MEMORY_GROUP_RATIO.getKey() + " group ratio is not equals with group size");
        List<MemoryGroup> groups = memoryGroups();
        for (int i = 0; i < groups.size(); i++) {
            groups.get(i).setRatio(ratios[i]);
        }
        LOGGER.info("MemoryGroup ratio : {}", groupRatios);
    }

    synchronized void resetMemory(long memorySize, int chunkSize, MemoryMode memoryMode) {
        if (memoryMode != MemoryMode.OFF_HEAP) {
            return;
        }

        List<MemoryGroup> groupList = memoryGroups();
        groupList.forEach(e -> e.setBaseBytes(memorySize, chunkSize, memoryMode));

        long oldShared = totalOffHeapSharedMemory;
        totalOffHeapSharedMemory =
            memorySize - groupList.stream().mapToLong(e -> e.baseBytes()).sum();
        SHARED_OFF_HEAP_FREE_BYTES.getAndAdd(totalOffHeapSharedMemory - oldShared);

        LOGGER.info("[default:{}, shuffle:{}, state:{}, totalShared:{}, sharedFreeBytes:{}]",
            DEFAULT.baseBytes(), SHUFFLE.baseBytes(), STATE.baseBytes(), totalOffHeapSharedMemory,
            SHARED_OFF_HEAP_FREE_BYTES.get());
    }

    private static void register(MemoryGroup group) {
        GROUPS.put(group.getName(), group);
    }

    private static String dump() {
        StringBuilder sb = new StringBuilder();
        GROUPS.forEach((k, v) -> {
            sb.append(v.toString()).append("\n");
        });
        return sb.toString();
    }

    void clear() {
        GROUPS.forEach((k, v) -> v.reset());
        SHARED_OFF_HEAP_FREE_BYTES.set(0);
        totalOffHeapSharedMemory = 0;
        manger = null;
    }

    public List<MemoryGroup> memoryGroups() {
        return Lists.newArrayList(GROUPS.values());
    }

    public long getCurrentSharedFreeBytes() {
        return SHARED_OFF_HEAP_FREE_BYTES.get();
    }

}
