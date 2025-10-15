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

package org.apache.geaflow.dsl.runtime.expression.subquery;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.geaflow.dsl.runtime.traversal.data.ParameterRequest;
import org.apache.geaflow.dsl.runtime.traversal.path.ITreePath;

public class CallContext {

    // requestId -> (vertexId, treePath)
    private final Map<Object, Map<Object, ITreePath>> paths;

    // vertexId -> request list
    private final Map<Object, List<ParameterRequest>> requests;

    public CallContext(Map<Object, Map<Object, ITreePath>> paths, Map<Object, List<ParameterRequest>> requests) {
        this.paths = Objects.requireNonNull(paths);
        this.requests = Objects.requireNonNull(requests);
    }

    public CallContext() {
        this(new HashMap<>(), new HashMap<>());
    }

    public void addPath(Object requestId, Object vertexId, ITreePath treePath) {
        paths.computeIfAbsent(requestId, r -> new HashMap<>()).put(vertexId, treePath);
    }

    public ITreePath getPath(Object requestId, Object vertexId) {
        if (paths.containsKey(requestId)) {
            Map<Object, ITreePath> vertexTreePaths = paths.get(requestId);
            if (vertexTreePaths != null) {
                return vertexTreePaths.get(vertexId);
            }
        }
        return null;
    }

    public void addRequest(Object vertexId, ParameterRequest request) {
        if (request != null) {
            requests.computeIfAbsent(vertexId, v -> new ArrayList<>()).add(request);
        }
    }

    public List<ParameterRequest> getRequests(Object vertexId) {
        return requests.get(vertexId);
    }

    public void reset() {
        paths.clear();
        requests.clear();
    }
}
