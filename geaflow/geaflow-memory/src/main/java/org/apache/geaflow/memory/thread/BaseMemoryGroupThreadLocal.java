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

package org.apache.geaflow.memory.thread;

import com.google.common.collect.Maps;
import io.netty.util.concurrent.FastThreadLocal;
import io.netty.util.concurrent.FastThreadLocalThread;
import io.netty.util.internal.InternalThreadLocalMap;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.Set;
import org.apache.geaflow.memory.MemoryGroup;
import org.apache.geaflow.memory.PlatformDependent;

/**
 * This Class is a subClass of Netty FastThreadLocal, and adapted to MemoryGroup.
 */
public abstract class BaseMemoryGroupThreadLocal<V> extends FastThreadLocal<V> {

    private static final int VARIABLES_TO_REMOVE_INDEX = InternalThreadLocalMap.nextVariableIndex();
    private final int index = InternalThreadLocalMap.nextVariableIndex();

    /**
     * Returns the current value for the current thread.
     */
    @SuppressWarnings("unchecked")
    public final V get(MemoryGroup group) {
        InternalThreadLocalMap threadLocalMap = InternalThreadLocalMap.get();
        Object v = threadLocalMap.indexedVariable(index);
        if (v != InternalThreadLocalMap.UNSET) {
            Map<MemoryGroup, V> map = (Map<MemoryGroup, V>) v;
            if (map.containsKey(group)) {
                return map.get(group);
            }
            V value = null;
            try {
                value = initialValue(group);
                map.put(group, value);
            } catch (Exception e) {
                PlatformDependent.throwException(e);
            }
            return value;
        }

        V value = initialize(threadLocalMap, group);
        registerCleaner(threadLocalMap);
        return value;
    }

    private void registerCleaner(final InternalThreadLocalMap threadLocalMap) {
        Thread current = Thread.currentThread();
        if (FastThreadLocalThread.willCleanupFastThreadLocals(current) || threadLocalMap.isCleanerFlagSet(index)) {
            return;
        }

        threadLocalMap.setCleanerFlag(index);
    }

    private V initialize(InternalThreadLocalMap threadLocalMap, MemoryGroup group) {
        V v = null;
        try {
            v = initialValue(group);
        } catch (Exception e) {
            PlatformDependent.throwException(e);
        }
        Map<MemoryGroup, V> map = Maps.newHashMap();
        map.put(group, v);

        threadLocalMap.setIndexedVariable(index, map);
        addToVariablesToRemove(threadLocalMap, this);
        return v;
    }

    private static void addToVariablesToRemove(InternalThreadLocalMap threadLocalMap, FastThreadLocal<?> variable) {
        Object v = threadLocalMap.indexedVariable(VARIABLES_TO_REMOVE_INDEX);
        Set<FastThreadLocal<?>> variablesToRemove;
        if (v == InternalThreadLocalMap.UNSET || v == null) {
            variablesToRemove = Collections.newSetFromMap(new IdentityHashMap<FastThreadLocal<?>, Boolean>());
            threadLocalMap.setIndexedVariable(VARIABLES_TO_REMOVE_INDEX, variablesToRemove);
        } else {
            variablesToRemove = (Set<FastThreadLocal<?>>) v;
        }

        variablesToRemove.add(variable);
    }

    /**
     * Returns the initial value for this thread-local variable.
     */
    protected abstract V initialValue(MemoryGroup group) throws Exception;

    protected abstract void notifyRemove(V v);

    protected void onRemoval(Object v) throws Exception {
        if (v instanceof Map) {
            ((Map) v).forEach((k, val) -> notifyRemove((V) val));
        }
    }
}
