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

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.apache.geaflow.common.exception.GeaflowRuntimeException;

public class CycleResponseEventPool<T> {

    private LinkedBlockingQueue<T> eventQueue;
    private static final int WAITING_TIME_OUT = 10;

    public CycleResponseEventPool() {
        eventQueue = new LinkedBlockingQueue();
    }

    public void notifyEvent(T event) {
        try {
            eventQueue.put(event);
        } catch (InterruptedException e) {
            throw new GeaflowRuntimeException(e);
        }
    }

    public T waitEvent() {
        while (true) {
            try {
                // Wait until get available response.
                T event = eventQueue.poll(WAITING_TIME_OUT, TimeUnit.MILLISECONDS);
                if (event == null) {
                    continue;
                }
                return event;
            } catch (InterruptedException e) {
                throw new GeaflowRuntimeException(e);
            }
        }
    }

    public void clear() {
        eventQueue.clear();
    }
}
