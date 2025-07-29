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

package org.apache.geaflow.cluster.driver;

import java.util.HashMap;
import java.util.Map;
import org.apache.geaflow.cluster.common.IDispatcher;
import org.apache.geaflow.cluster.common.IEventListener;
import org.apache.geaflow.cluster.protocol.ICycleResponseEvent;
import org.apache.geaflow.cluster.protocol.IEvent;
import org.apache.geaflow.common.exception.GeaflowRuntimeException;

public class DriverEventDispatcher implements IDispatcher {

    private Map<Long, IEventListener> eventListenerMap;

    public DriverEventDispatcher() {
        this.eventListenerMap = new HashMap<>();
    }

    public void dispatch(IEvent event) {
        ICycleResponseEvent doneEvent = (ICycleResponseEvent) event;
        IEventListener eventListener = eventListenerMap.get(doneEvent.getSchedulerId());
        if (eventListener == null) {
            throw new GeaflowRuntimeException(String.format("event %s do not find handle listener %s", event, doneEvent.getSchedulerId()));
        }
        eventListener.handleEvent(event);
    }

    public void registerListener(long schedulerId, IEventListener eventListener) {
        this.eventListenerMap.put(schedulerId, eventListener);
    }

    public void removeListener(long schedulerId) {
        this.eventListenerMap.remove(schedulerId);
    }
}
