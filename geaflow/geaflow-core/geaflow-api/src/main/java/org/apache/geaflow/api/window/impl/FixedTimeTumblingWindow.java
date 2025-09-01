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

package org.apache.geaflow.api.window.impl;

import org.apache.geaflow.api.window.ITumblingWindow;
import org.apache.geaflow.api.window.WindowType;

public class FixedTimeTumblingWindow<T> implements ITumblingWindow<T> {

    private final long timeWindowInSecond;
    private long windowId;

    public FixedTimeTumblingWindow(long timeWindowInSecond) {
        this.timeWindowInSecond = timeWindowInSecond;
    }

    public long getTimeWindowSize() {
        return timeWindowInSecond;
    }

    @Override
    public long windowId() {
        return windowId;
    }

    @Override
    public void initWindow(long windowId) {
        this.windowId = windowId;
    }

    @Override
    public long assignWindow(T value) {
        return windowId;
    }

    @Override
    public WindowType getType() {
        return WindowType.FIXED_TIME_TUMBLING_WINDOW;
    }
}
