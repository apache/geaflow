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

package org.apache.geaflow.dsl.udf.graph.gcn;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class GCNState implements Serializable {

    private GCNAccumulator accumulator;
    private final Map<Object, Integer> minDepthByRoot;
    private boolean dynamicExecution;

    public GCNState() {
        this.minDepthByRoot = new HashMap<>();
    }

    public GCNAccumulator getAccumulator() {
        return accumulator;
    }

    public void initAccumulator(Object rootId) {
        if (accumulator == null) {
            accumulator = new GCNAccumulator(rootId);
        }
    }

    public Map<Object, Integer> getMinDepthByRoot() {
        return minDepthByRoot;
    }

    public boolean shouldExpand(Object rootId, int depth) {
        Integer knownDepth = minDepthByRoot.get(rootId);
        if (knownDepth != null && knownDepth <= depth) {
            return false;
        }
        minDepthByRoot.put(rootId, depth);
        return true;
    }

    public boolean isDynamicExecution() {
        return dynamicExecution;
    }

    public void setDynamicExecution(boolean dynamicExecution) {
        this.dynamicExecution = dynamicExecution;
    }
}
