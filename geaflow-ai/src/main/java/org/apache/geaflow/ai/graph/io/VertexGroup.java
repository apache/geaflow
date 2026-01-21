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

package org.apache.geaflow.ai.graph.io;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import org.apache.geaflow.ai.common.ErrorCode;

public class VertexGroup implements EntityGroup {

    public final VertexSchema vertexSchema;
    private final List<Vertex> vertices;
    private final Map<String, Integer> index;

    public VertexGroup(VertexSchema vertexSchema, List<Vertex> vertices) {
        this.vertexSchema = vertexSchema;
        this.vertices = vertices;
        this.index = new HashMap<>(vertices.size());
        buildIndex();
    }

    private void buildIndex() {
        int index = 0;
        for (Vertex v : vertices) {
            this.index.put(v.getId(), index);
            index++;
        }
    }

    public int addVertex(Vertex newVertex) {
        if (newVertex == null || newVertex.getId() == null) {
            return ErrorCode.GRAPH_ENTITY_GROUP_INSERT_FAILED;
        }
        if (index.containsKey(newVertex.getId())) {
            return ErrorCode.GRAPH_ENTITY_GROUP_INSERT_FAILED;
        }
        this.vertices.add(newVertex);
        this.index.put(newVertex.getId(), vertices.size() - 1);
        return ErrorCode.SUCCESS;
    }

    public int updateVertex(Vertex newVertex) {
        if (newVertex == null || newVertex.getId() == null) {
            return ErrorCode.GRAPH_ENTITY_GROUP_UPDATE_FAILED;
        }
        if (!index.containsKey(newVertex.getId())) {
            return ErrorCode.GRAPH_ENTITY_GROUP_UPDATE_FAILED;
        }
        int offset = index.get(newVertex.getId());
        this.vertices.set(offset, newVertex);
        return ErrorCode.SUCCESS;
    }

    public int removeVertex(String id) {
        if (id == null) {
            return ErrorCode.GRAPH_ENTITY_GROUP_REMOVE_FAILED;
        }
        if (!index.containsKey(id)) {
            return ErrorCode.GRAPH_ENTITY_GROUP_REMOVE_FAILED;
        }
        int offset = index.get(id);
        vertices.set(offset, null);
        index.remove(id);
        return ErrorCode.SUCCESS;
    }

    public VertexSchema getVertexSchema() {
        return vertexSchema;
    }

    public List<Vertex> getVertices() {
        List<Vertex> vertices = this.vertices.stream()
            .filter(Objects::nonNull).collect(Collectors.toList());
        return vertices;
    }

    public Vertex getVertex(String id) {
        if (index.get(id) != null) {
            return vertices.get(index.get(id));
        } else {
            return null;
        }
    }
}
