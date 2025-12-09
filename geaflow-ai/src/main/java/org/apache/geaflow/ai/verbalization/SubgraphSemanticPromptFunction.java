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

package org.apache.geaflow.ai.verbalization;

import org.apache.geaflow.ai.graph.GraphAccessor;
import org.apache.geaflow.ai.graph.GraphEdge;
import org.apache.geaflow.ai.graph.GraphEntity;
import org.apache.geaflow.ai.graph.GraphVertex;
import org.apache.geaflow.ai.graph.io.GraphSchema;
import org.apache.geaflow.ai.subgraph.SubGraph;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class SubgraphSemanticPromptFunction implements VerbalizationFunction {

    private final GraphAccessor graphAccessor;

    public SubgraphSemanticPromptFunction(GraphAccessor accessor) {
        this.graphAccessor = Objects.requireNonNull(accessor);
    }

    @Override
    public String verbalize(SubGraph subGraph) {
        if (subGraph == null || subGraph.getGraphEntityList().isEmpty()) {
            return "Empty.";
        }
        List<String> sentences = new ArrayList<>();
        GraphSchema schema = graphAccessor.getGraphSchema();
        for (GraphEntity entity : subGraph.getGraphEntityList()) {
            if (entity instanceof GraphVertex) {
                GraphVertex graphVertex = (GraphVertex) entity;
                sentences.add(schema.getPrompt(graphVertex));
            } else if (entity instanceof GraphEdge) {
                GraphEdge graphEdge = (GraphEdge) entity;
                GraphVertex start = graphAccessor.getVertex(null, graphEdge.getEdge().getSrcId());
                GraphVertex end = graphAccessor.getVertex(null, graphEdge.getEdge().getDstId());
                sentences.add(schema.getPrompt(graphEdge, start, end));
            }
        }
        return String.join("  ", sentences);
    }
}
