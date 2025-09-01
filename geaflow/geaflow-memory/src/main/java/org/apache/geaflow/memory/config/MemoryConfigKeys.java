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

package org.apache.geaflow.memory.config;

import java.io.Serializable;
import org.apache.geaflow.common.config.ConfigKey;
import org.apache.geaflow.common.config.ConfigKeys;
import org.apache.geaflow.memory.DirectMemory;

public class MemoryConfigKeys implements Serializable {

    public static final long JVM_MAX_DIRECT_MEMORY = DirectMemory.maxDirectMemory0();

    public static final ConfigKey OFF_HEAP_MEMORY_SIZE_MB = ConfigKeys
        .key("geaflow.memory.off.heap.mb")
        .defaultValue(0L)
        .description("off heap memory mb, default 0");

    public static final ConfigKey ON_HEAP_MEMORY_SIZE_MB = ConfigKeys
        .key("geaflow.memory.on.heap.mb")
        .defaultValue(0L)
        .description("on heap memory mb, default 0");

    public static final ConfigKey MAX_DIRECT_MEMORY_SIZE = ConfigKeys
        .key("geaflow.memory.max.direct.size")
        .defaultValue((long) (JVM_MAX_DIRECT_MEMORY * 0.8))
        .description("max direct memory size, default 0.8 * JVM_MAX_DIRECT_MEMORY");

    public static final ConfigKey MEMORY_PAGE_SIZE = ConfigKeys
        .key("geaflow.memory.page.size")
        .defaultValue(8192)
        .description("memory page size, default 8192 Byte");

    public static final ConfigKey MEMORY_MAX_ORDER = ConfigKeys
        .key("geaflow.memory.max.order")
        .defaultValue(11)
        .description("memory max order, default 11");

    public static final ConfigKey MEMORY_POOL_SIZE = ConfigKeys
        .key("geaflow.memory.pool.size")
        .defaultValue(0)
        .description("inner memory pool size, default 0");

    public static final ConfigKey MEMORY_GROUP_RATIO = ConfigKeys
        .key("geaflow.memory.group.ratio")
        .defaultValue("10:*:*")
        .description("format shuffle:state:default=10:*:*, shuffle=10%, *=shared memory");

    public static final ConfigKey MEMORY_TRIM_GAP_MINUTE = ConfigKeys
        .key("geaflow.memory.trim.gap.minute")
        .defaultValue(30)
        .description("auto check memory, and trim gap, default 30min");

    public static final ConfigKey MEMORY_DEBUG_ENABLE = ConfigKeys
        .key("geaflow.memory.debug.enable")
        .defaultValue(false)
        .description("memory manager mist print, default false");

    public static final ConfigKey MEMORY_AUTO_ADAPT_ENABLE = ConfigKeys
        .key("geaflow.memory.auto.adapt.enable")
        .defaultValue(true)
        .description("auto memory scale from direct to onHeap, default true");
}
