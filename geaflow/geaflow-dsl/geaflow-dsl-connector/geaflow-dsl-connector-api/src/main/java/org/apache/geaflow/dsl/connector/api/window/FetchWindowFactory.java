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

import org.apache.geaflow.api.window.IWindow;
import org.apache.geaflow.api.window.impl.FixedTimeTumblingWindow;
import org.apache.geaflow.api.window.impl.SizeTumblingWindow;
import org.apache.geaflow.dsl.common.exception.GeaFlowDSLException;

/**
 * Convert to the fetch window from the common window.
 */
public class FetchWindowFactory {

    public static <T> FetchWindow createFetchWindow(IWindow<T> window) {
        switch (window.getType()) {
            case ALL_WINDOW:
                return new AllFetchWindow(window.windowId());
            case SIZE_TUMBLING_WINDOW:
                return new SizeFetchWindow(window.windowId(), ((SizeTumblingWindow<T>) window).getSize());
            case FIXED_TIME_TUMBLING_WINDOW:
                return new TimeFetchWindow(window.windowId(), ((FixedTimeTumblingWindow<T>) window).getTimeWindowSize());
            default:
                throw new GeaFlowDSLException("Not support window type:{}", window.getType());
        }
    }

}
