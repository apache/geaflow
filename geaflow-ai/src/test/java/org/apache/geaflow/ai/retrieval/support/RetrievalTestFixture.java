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

package org.apache.geaflow.ai.retrieval.support;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.geaflow.ai.graph.io.GraphSchema;
import org.apache.geaflow.ai.graph.io.Vertex;
import org.apache.geaflow.ai.graph.io.VertexSchema;

/** Two independent facts with fixed identity and insertion order. */
public final class RetrievalTestFixture {

    public static final String GRAPH_NAME = "week1-keyword-graph";
    public static final String CHUNK_LABEL = "chunk";
    public static final String CONFUCIUS_ID = "confucius-1";
    public static final String ASTRONOMY_ID = "astronomy-1";

    public GraphSchema graphSchema() {
        GraphSchema schema = new GraphSchema();
        schema.setName(GRAPH_NAME);
        return schema;
    }

    public GraphSchema getGraphSchema() {
        return graphSchema();
    }

    public VertexSchema vertexSchema() {
        return new VertexSchema(CHUNK_LABEL, "id", Collections.singletonList("text"));
    }

    public VertexSchema getVertexSchema() {
        return vertexSchema();
    }

    public List<Vertex> vertices() {
        return Arrays.asList(
            new Vertex(CHUNK_LABEL, CONFUCIUS_ID, Collections.singletonList("Confucius taught ethics.")),
            new Vertex(CHUNK_LABEL, ASTRONOMY_ID, Collections.singletonList("Astronomy studies stars.")));
    }

    public List<Vertex> getVertices() {
        return vertices();
    }
}
