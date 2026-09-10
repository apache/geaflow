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

package org.apache.geaflow.api.graph.sampling;

import java.io.Serializable;
import java.util.Objects;

/** A request to expand one vertex for one sampling layer. */
public class SubgraphSamplingRequest<K> implements Serializable {

    private final K rootId;
    private final int depth;

    public SubgraphSamplingRequest(K rootId, int depth) {
        if (depth < 1) {
            throw new IllegalArgumentException("sampling request depth must be greater than zero");
        }
        this.rootId = Objects.requireNonNull(rootId, "rootId");
        this.depth = depth;
    }

    public K getRootId() {
        return rootId;
    }

    public int getDepth() {
        return depth;
    }
}
