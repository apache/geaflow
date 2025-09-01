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

package org.apache.geaflow.dsl.connector.api.window;

import org.apache.geaflow.api.window.WindowType;

/**
 * Time window.
 */
public class TimeFetchWindow extends AbstractFetchWindow {

    private final long windowSizeInSecond;

    public TimeFetchWindow(long windowId, long windowSizeInSecond) {
        super(windowId);
        this.windowSizeInSecond = windowSizeInSecond;
    }

    // include
    public long getStartWindowTime(long startTime) {
        return startTime + windowId * windowSizeInSecond * 1000;
    }

    // exclude
    public long getEndWindowTime(long startTime) {
        return startTime + (windowId + 1) * windowSizeInSecond * 1000;
    }

    @Override
    public long windowSize() {
        return windowSizeInSecond;
    }

    @Override
    public WindowType getType() {
        return WindowType.FIXED_TIME_TUMBLING_WINDOW;
    }
}
