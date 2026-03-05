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

package org.apache.geaflow.ai.search;

import java.util.*;
import org.apache.geaflow.ai.index.vector.IVector;
import org.apache.geaflow.ai.index.vector.VectorType;

public class VectorSearch {

    public final String memoryId;

    public final String sessionId;

    public final Map<VectorType, List<IVector>> vectorList = new LinkedHashMap<>();

    public VectorSearch(String memoryId, String sessionId) {
        this.memoryId = memoryId;
        this.sessionId = sessionId;
    }

    public void addVector(IVector vector) {
        addVector(Collections.singletonList(vector));
    }

    public void addVector(List<IVector> vectors) {
        if (vectors == null) {
            return;
        }
        for (IVector v : vectors) {
            if (v != null) {
                vectorList.computeIfAbsent(v.getType(),
                        k -> new ArrayList<>()).add(v);
            }
        }
    }

    @Override
    public String toString() {
        return "VectorSearch{"
                + "memoryId='" + memoryId + '\''
                + ", sessionId='" + sessionId + '\''
                + ", vectorList=" + vectorList
                + '}';
    }

    public String getMemoryId() {
        return memoryId;
    }

    public String getSessionId() {
        return sessionId;
    }

    public Map<VectorType, List<IVector>> getVectorMap() {
        return vectorList;
    }
}
