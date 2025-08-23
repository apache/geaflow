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

package org.apache.geaflow.console.core.model.data;

import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.apache.commons.collections.MapUtils;
import org.apache.geaflow.console.core.model.plugin.config.GeaflowPluginConfig;

@Getter
@Setter
@NoArgsConstructor
public class GeaflowGraph extends GeaflowData {

    private final Map<String, GeaflowVertex> vertices = new LinkedHashMap<>();

    private final Map<String, GeaflowEdge> edges = new LinkedHashMap<>();

    private List<GeaflowEndpoint> endpoints = new ArrayList<>();

    private GeaflowPluginConfig pluginConfig;

    public GeaflowGraph(String name, String comment) {
        super.name = name;
        super.comment = comment;
    }

    public void addVertices(List<GeaflowVertex> vertices) {
        for (GeaflowVertex vertex : vertices) {
            this.vertices.put(vertex.getName(), vertex);
        }
    }

    public void addEdges(List<GeaflowEdge> edges) {
        for (GeaflowEdge edge : edges) {
            this.edges.put(edge.getName(), edge);
        }
    }

    public void addVertex(GeaflowVertex vertex) {
        vertices.put(vertex.getName(), vertex);
    }

    public void addEdge(GeaflowEdge edge) {
        edges.put(edge.getName(), edge);
    }


    public void removeVertex(String vertexName) {
        vertices.remove(vertexName);
    }

    public void removeEdge(String edgeName) {
        edges.remove(edgeName);
    }

    @Override
    public void validate() {
        super.validate();
        Preconditions.checkNotNull(pluginConfig, "pluginConfig is null");
        Preconditions.checkArgument(MapUtils.isNotEmpty(vertices), "Graph needs at least one vertex");
        Preconditions.checkArgument(MapUtils.isNotEmpty(edges), "Graph needs at least one edge");
    }

    public int getShardCount() {
        String shardCount = (String) pluginConfig.getConfig().get("geaflow.dsl.graph.store.shard.count");
        if (shardCount != null) {
            return Integer.parseInt(shardCount);
        }

        return Integer.parseInt((String) pluginConfig.getConfig().getOrDefault("shardCount", "2"));
    }
}
