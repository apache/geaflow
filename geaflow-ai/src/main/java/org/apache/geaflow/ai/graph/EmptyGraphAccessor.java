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

package org.apache.geaflow.ai.graph;

import java.util.ArrayList;
import java.util.Iterator;

public class EmptyGraphAccessor implements GraphAccessor {

    @Override
    public GraphVertex getVertex(EntityId entityId) {
        throw new RuntimeException("Not support.");
    }

    @Override
    public Iterator<GraphVertex> scanVertex() {
        return new ArrayList<GraphVertex>().iterator();
    }

    @Override
    public Iterator<GraphEdge> scanEdge(GraphVertex vertex) {
        return new ArrayList<GraphEdge>().iterator();
    }

    @Override
    public GraphAccessor copy() {
        return new EmptyGraphAccessor();
    }

    @Override
    public String getType() {
        return EmptyGraphAccessor.class.getSimpleName();
    }
}
