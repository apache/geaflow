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

package org.apache.geaflow.console.core.service.llm;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang3.StringUtils;
import org.apache.geaflow.console.common.util.VelocityUtil;
import org.apache.geaflow.console.core.model.GeaflowId;
import org.apache.geaflow.console.core.model.data.GeaflowEdge;
import org.apache.geaflow.console.core.model.data.GeaflowEndpoint;
import org.apache.geaflow.console.core.model.data.GeaflowGraph;
import org.apache.geaflow.console.core.model.data.GeaflowVertex;

public class GraphSchemaTranslator {

    private static final String VERTEX_TEMPLATE = "template/graphSchema.vm";

    private static final String END_FLAG = "END_FLAG";


    public static String translateGraphSchema(GeaflowGraph graph) {
        HashMap<String, Object> velocityMap = new HashMap<>();

        Map<String, GeaflowVertex> vertexMap = graph.getVertices().values().stream().collect(Collectors.toMap(GeaflowId::getId, e -> e));
        Map<String, GeaflowEdge> edgeMap = graph.getEdges().values().stream().collect(Collectors.toMap(GeaflowId::getId, e -> e));

        Map<String, NameEndpoint> endpointMap = new HashMap<>();
        // format endpoints
        for (GeaflowEndpoint endpoint : graph.getEndpoints()) {
            GeaflowEdge edge = edgeMap.get(endpoint.getEdgeId());
            GeaflowVertex source = vertexMap.get(endpoint.getSourceId());
            GeaflowVertex target = vertexMap.get(endpoint.getTargetId());
            if (edge != null && source != null && target != null) {
                endpointMap.put(edge.getName(), new NameEndpoint(source.getName(), target.getName()));
            }
        }

        velocityMap.put("vertices", graph.getVertices().values());
        velocityMap.put("edges", graph.getEdges().values());
        velocityMap.put("endpoints", endpointMap);
        velocityMap.put("endFlag", END_FLAG);
        String s = VelocityUtil.applyResource(VERTEX_TEMPLATE, velocityMap);
        return StringUtils.replacePattern(s, ",\n*\\s*" + END_FLAG, "");
    }


    @AllArgsConstructor
    @Setter
    @Getter
    public static class NameEndpoint {

        String sourceName;
        String targetName;
    }
}
