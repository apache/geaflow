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

package org.apache.geaflow.dsl.common.util;

import com.google.common.base.Preconditions;
import org.apache.geaflow.api.window.IWindow;
import org.apache.geaflow.api.window.impl.AllWindow;
import org.apache.geaflow.api.window.impl.FixedTimeTumblingWindow;
import org.apache.geaflow.api.window.impl.SizeTumblingWindow;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.common.config.keys.DSLConfigKeys;

public class Windows {

    public static final long SIZE_OF_ALL_WINDOW = -1L;

    public static <T> IWindow<T> createWindow(Configuration configuration) {
        long batchWindowSize = Integer.MIN_VALUE;
        if (configuration.contains(DSLConfigKeys.GEAFLOW_DSL_WINDOW_SIZE)) {
            batchWindowSize = configuration.getLong(DSLConfigKeys.GEAFLOW_DSL_WINDOW_SIZE);
            Preconditions.checkState(batchWindowSize != 0, "Window size should not be zero!");
        }
        long timeWindowDuration = -1;
        if (configuration.contains(DSLConfigKeys.GEAFLOW_DSL_TIME_WINDOW_SIZE)) {
            timeWindowDuration = configuration.getLong(DSLConfigKeys.GEAFLOW_DSL_TIME_WINDOW_SIZE);
            Preconditions.checkState(timeWindowDuration > 0, "Time Window size should not be positive!");
        }
        Preconditions.checkState(!(batchWindowSize >= SIZE_OF_ALL_WINDOW && timeWindowDuration > 0),
            "Only one of window can exist! size window:%s, time window:%s", batchWindowSize, timeWindowDuration);
        if (batchWindowSize == SIZE_OF_ALL_WINDOW) {
            return AllWindow.getInstance();
        } else if (batchWindowSize > 0) {
            return new SizeTumblingWindow<>(batchWindowSize);
        } else if (timeWindowDuration > 0) {
            return new FixedTimeTumblingWindow<>(timeWindowDuration);
        } else {
            // use default
            return new SizeTumblingWindow<>((Long) DSLConfigKeys.GEAFLOW_DSL_WINDOW_SIZE.getDefaultValue());
        }
    }
}
