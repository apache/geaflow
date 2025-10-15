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

import java.util.ArrayList;
import java.util.List;
import org.apache.geaflow.stats.collector.StatsCollectorFactory;
import org.apache.geaflow.stats.model.ExceptionLevel;

public abstract class AbstractPodHandler implements IPodEventHandler {

    protected List<IEventListener> listeners;

    public AbstractPodHandler() {
        this.listeners = new ArrayList<>();
    }

    public void addListener(IEventListener listener) {
        listeners.add(listener);
    }

    public void notifyListeners(PodEvent event) {
        for (IEventListener listener : listeners) {
            listener.onEvent(event);
        }
    }

    protected void reportPodEvent(PodEvent event, ExceptionLevel level, String message) {
        String eventMessage = buildEventMessage(event, message);
        StatsCollectorFactory.getInstance().getEventCollector()
            .reportEvent(level, event.getEventKind().name(), eventMessage);
    }

    private String buildEventMessage(PodEvent event, String message) {
        return message + "\n" + event.toString();
    }

}
