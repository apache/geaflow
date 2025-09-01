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

package org.apache.geaflow.operator.impl.graph.traversal.dynamic;


import org.apache.geaflow.api.context.RuntimeContext;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.common.config.keys.DSLConfigKeys;

public class DynamicGraphHelper {

    public static boolean enableIncrTraversal(int maxIterationCount, int startIdSize, Configuration configuration) {
        if (configuration != null) {
            boolean res = configuration.getBoolean(DSLConfigKeys.ENABLE_INCR_TRAVERSAL);
            if (!res) {
                return false;
            }
        }

        int traversalThreshold = configuration.getInteger(DSLConfigKeys.INCR_TRAVERSAL_ITERATION_THRESHOLD);
        // when maxIterationCount <=2 no need to include subGraph, since 1 hop is already included in the incr edges.
        return maxIterationCount > 2 && maxIterationCount <= traversalThreshold && startIdSize == 0;
    }

    public static boolean enableIncrTraversalRuntime(RuntimeContext runtimeContext) {
        long windowId = runtimeContext.getWindowId();
        if (windowId == 1) {
            // the first window not need evolve
            return false;
        }
        long window = runtimeContext.getConfiguration().getLong(DSLConfigKeys.INCR_TRAVERSAL_WINDOW);
        if (window == -1) {
            // default do incr
            return true;
        } else {
            return windowId > window;
        }
    }
}
