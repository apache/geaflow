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

package org.apache.geaflow.operator.impl.graph.algo.vc.context.dynamic;

import java.util.List;
import org.apache.geaflow.api.graph.function.vc.base.IncVertexCentricFunction.TemporaryGraph;
import org.apache.geaflow.model.graph.edge.IEdge;
import org.apache.geaflow.model.graph.vertex.IVertex;
import org.apache.geaflow.model.graph.vertex.impl.ValueVertex;
import org.apache.geaflow.operator.impl.graph.compute.dynamic.cache.TemporaryGraphCache;

public class IncTemporaryGraph<K, VV, EV> implements TemporaryGraph<K, VV, EV> {

    private K vertexId;
    private TemporaryGraphCache<K, VV, EV> temporaryGraphCache;

    public IncTemporaryGraph(TemporaryGraphCache<K, VV, EV> temporaryGraphCache) {
        this.temporaryGraphCache = temporaryGraphCache;
    }

    public void init(K vertexId) {
        this.vertexId = vertexId;
    }

    @Override
    public IVertex<K, VV> getVertex() {
        return temporaryGraphCache.getVertex(vertexId);
    }

    @Override
    public List<IEdge<K, EV>> getEdges() {
        return temporaryGraphCache.getEdges(vertexId);
    }

    @Override
    public void updateVertexValue(VV value) {
        IVertex<K, VV> valueVertex = temporaryGraphCache.getVertex(vertexId);
        if (valueVertex == null) {
            valueVertex = new ValueVertex<>(vertexId, value);
        } else {
            valueVertex = valueVertex.withValue(value);
        }
        temporaryGraphCache.addVertex(valueVertex);
    }

}
