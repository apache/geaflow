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

import org.apache.geaflow.ai.graph.GraphEdge;
import org.apache.geaflow.ai.graph.GraphEntity;
import org.apache.geaflow.ai.graph.GraphVertex;
import org.apache.geaflow.ai.verbalization.PromptFormatter;

import java.util.ArrayList;
import java.util.List;

public class GraphSchema implements Schema {

    private PromptFormatter promptFormatter;

    public GraphSchema() {
        this.vertexSchemaList = new ArrayList<>();
        this.edgeSchemaList = new ArrayList<>();
    }

    public GraphSchema(List<VertexSchema> vertexSchemaList, List<EdgeSchema> edgeSchemaList) {
        this.vertexSchemaList = vertexSchemaList;
        this.edgeSchemaList = edgeSchemaList;
    }

    private final List<VertexSchema> vertexSchemaList;
    private final List<EdgeSchema> edgeSchemaList;

    public void addVertex(VertexSchema vertexSchema) {
        vertexSchemaList.add(vertexSchema);
    }

    public void addEdge(EdgeSchema edgeSchema) {
        edgeSchemaList.add(edgeSchema);
    }

    public Schema getSchema(String label) {
        for (VertexSchema vs : vertexSchemaList) {
            if (label.equals(vs.getLabel())) {
                return vs;
            }
        }
        for (EdgeSchema vs : edgeSchemaList) {
            if (label.equals(vs.getLabel())) {
                return vs;
            }
        }
        return null;
    }

    @Override
    public List<String> getFields() {
        return null;
    }

    public List<VertexSchema> getVertexSchemaList() {
        return vertexSchemaList;
    }

    public List<EdgeSchema> getEdgeSchemaList() {
        return edgeSchemaList;
    }

    @Override
    public String getName() {
        return "GRAPH";
    }

    public void setPromptFormatter(PromptFormatter promptFormatter) {
        this.promptFormatter = promptFormatter;
    }

    public String getPrompt(GraphVertex entity) {
        if (promptFormatter == null) {
            return entity.toString();
        } else {
            return promptFormatter.prompt(entity);
        }
    }

    public String getPrompt(GraphEdge entity, GraphVertex start, GraphVertex end) {
        if (promptFormatter == null) {
            return entity.toString();
        } else {
            return promptFormatter.prompt(entity, start, end);
        }
    }
}
