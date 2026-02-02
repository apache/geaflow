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
import org.apache.commons.lang3.StringUtils;
import org.apache.geaflow.ai.common.ErrorCode;
import org.apache.geaflow.ai.graph.io.*;

public class MemoryMutableGraph implements MutableGraph {

    private final MemoryGraph graph;

    public MemoryMutableGraph(MemoryGraph graph) {
        this.graph = graph;
    }

    @Override
    public int removeVertex(String label, String id) {
        return this.graph.removeVertex(label, id);
    }

    @Override
    public int updateVertex(Vertex newVertex) {
        return this.graph.updateVertex(newVertex);
    }

    @Override
    public int addVertex(Vertex newVertex) {
        return this.graph.addVertex(newVertex);
    }

    @Override
    public int removeEdge(Edge edge) {
        return this.graph.removeEdge(edge);
    }

    @Override
    public int addEdge(Edge newEdge) {
        return this.graph.addEdge(newEdge);
    }

    @Override
    public GraphSchema getSchema() {
        return this.graph.getGraphSchema();
    }

    @Override
    public int addVertexSchema(VertexSchema vertexSchema) {
        if (vertexSchema == null || StringUtils.isBlank(vertexSchema.getLabel())) {
            return ErrorCode.GRAPH_ADD_VERTEX_SCHEMA_FAILED;
        }
        for (VertexSchema existSchema : this.getSchema().getVertexSchemaList()) {
            if (existSchema.getLabel().equals(vertexSchema.getLabel())) {
                return ErrorCode.GRAPH_ADD_VERTEX_SCHEMA_FAILED;
            }
        }
        for (EdgeSchema existSchema : this.getSchema().getEdgeSchemaList()) {
            if (existSchema.getLabel().equals(vertexSchema.getLabel())) {
                return ErrorCode.GRAPH_ADD_VERTEX_SCHEMA_FAILED;
            }
        }
        if (this.graph.entities.get(vertexSchema.getLabel()) != null) {
            return ErrorCode.GRAPH_ADD_VERTEX_SCHEMA_FAILED;
        }
        this.graph.getGraphSchema().addVertex(vertexSchema);
        this.graph.entities.put(vertexSchema.getLabel(), new VertexGroup(vertexSchema, new ArrayList<>()));
        return ErrorCode.SUCCESS;
    }

    @Override
    public int addEdgeSchema(EdgeSchema edgeSchema) {
        if (edgeSchema == null || StringUtils.isBlank(edgeSchema.getLabel())) {
            return ErrorCode.GRAPH_ADD_EDGE_SCHEMA_FAILED;
        }
        for (VertexSchema existSchema : this.getSchema().getVertexSchemaList()) {
            if (existSchema.getLabel().equals(edgeSchema.getLabel())) {
                return ErrorCode.GRAPH_ADD_EDGE_SCHEMA_FAILED;
            }
        }
        for (EdgeSchema existSchema : this.getSchema().getEdgeSchemaList()) {
            if (existSchema.getLabel().equals(edgeSchema.getLabel())) {
                return ErrorCode.GRAPH_ADD_EDGE_SCHEMA_FAILED;
            }
        }
        if (this.graph.entities.get(edgeSchema.getLabel()) != null) {
            return ErrorCode.GRAPH_ADD_EDGE_SCHEMA_FAILED;
        }
        this.graph.getGraphSchema().addEdge(edgeSchema);
        this.graph.entities.put(edgeSchema.getLabel(), new EdgeGroup(edgeSchema, new ArrayList<>()));
        return ErrorCode.SUCCESS;
    }
}
