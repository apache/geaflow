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

package org.apache.geaflow.runtime.core.scheduler;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.geaflow.cluster.common.IDispatcher;
import org.apache.geaflow.cluster.common.IEventListener;
import org.apache.geaflow.cluster.protocol.EventType;
import org.apache.geaflow.cluster.protocol.IEvent;
import org.apache.geaflow.common.exception.GeaflowDispatchException;
import org.apache.geaflow.common.exception.GeaflowRuntimeException;
import org.apache.geaflow.runtime.core.protocol.DoneEvent;
import org.apache.geaflow.runtime.core.scheduler.response.EventListenerKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SchedulerEventDispatcher implements IDispatcher {

    private static final Logger LOGGER = LoggerFactory.getLogger(SchedulerEventDispatcher.class);

    private Map<EventListenerKey, IEventListener> listeners;
    private String cycleLogTag;

    public SchedulerEventDispatcher(String cycleLogTag) {
        this.cycleLogTag = cycleLogTag;
        this.listeners = new ConcurrentHashMap<>();
    }

    @Override
    public void dispatch(IEvent event) throws GeaflowDispatchException {
        if (event.getEventType() != EventType.DONE) {
            throw new GeaflowRuntimeException(String.format("%s not support handle event %s",
                cycleLogTag, event));
        }
        DoneEvent doneEvent = (DoneEvent) event;
        getListener(doneEvent).handleEvent(doneEvent);
    }

    public void registerListener(EventListenerKey key, IEventListener eventListener) {
        LOGGER.info("{} register event listener {}", cycleLogTag, key);
        listeners.put(key, eventListener);
    }

    public void removeListener(EventListenerKey key) {
        LOGGER.info("{} remove event listener {}", cycleLogTag, key);
        listeners.remove(key);
    }

    private IEventListener getListener(DoneEvent event) {
        IEventListener listener;
        if ((listener = listeners.get(
            EventListenerKey.of(event.getCycleId(), event.getSourceEvent(), event.getWindowId()))) != null) {
            return listener;
        } else if ((listener = listeners.get(
            EventListenerKey.of(event.getCycleId(), event.getSourceEvent()))) != null) {
            return listener;
        } else if ((listener = listeners.get(EventListenerKey.of(event.getCycleId()))) != null) {
            return listener;
        }
        throw new GeaflowRuntimeException(String.format("%s not found any listener for event %s. current listeners %s",
            cycleLogTag, event, listeners.keySet()));
    }
}
