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

package org.apache.geaflow.cluster.k8s.handler;

import io.fabric8.kubernetes.client.Watcher.Action;
import java.util.HashMap;
import java.util.Map;
import org.apache.geaflow.common.config.Configuration;

public class PodHandlerRegistry {

    private final Map<Action, Map<EventKind, IPodEventHandler>> eventHandlerMap;
    private static PodHandlerRegistry INSTANCE;

    private PodHandlerRegistry(Configuration configuration) {
        this.eventHandlerMap = new HashMap<>();

        Map<EventKind, IPodEventHandler> modifiedHandlerMap = new HashMap<>();
        modifiedHandlerMap.put(EventKind.POD_OOM, new PodOOMHandler());
        modifiedHandlerMap.put(EventKind.POD_EVICTION, new PodEvictHandler(configuration));
        this.eventHandlerMap.put(Action.MODIFIED, modifiedHandlerMap);

        Map<EventKind, IPodEventHandler> addedHandlerMap = new HashMap<>();
        addedHandlerMap.put(EventKind.POD_ADDED, new PodAddedHandler());
        this.eventHandlerMap.put(Action.ADDED, addedHandlerMap);

        Map<EventKind, IPodEventHandler> deletedHandlerMap = new HashMap<>();
        deletedHandlerMap.put(EventKind.POD_DELETED, new PodDeletedHandler());
        this.eventHandlerMap.put(Action.DELETED, deletedHandlerMap);
    }

    public static synchronized PodHandlerRegistry getInstance(Configuration configuration) {
        if (INSTANCE == null) {
            INSTANCE = new PodHandlerRegistry(configuration);
        }
        return INSTANCE;
    }

    public void registerListener(Action action, EventKind eventKind, IEventListener listener) {
        ((AbstractPodHandler) eventHandlerMap.get(action).get(eventKind)).addListener(listener);
    }

    public Map<Action, Map<EventKind, IPodEventHandler>> getHandlerMap() {
        return eventHandlerMap;
    }

    public enum EventKind {
        POD_ADDED,
        POD_DELETED,
        POD_OOM,
        POD_EVICTION
    }

}
