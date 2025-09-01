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

package org.apache.geaflow.console.core.service.converter;

import java.util.List;
import org.apache.geaflow.console.common.dal.entity.GraphEntity;
import org.apache.geaflow.console.core.model.data.GeaflowEdge;
import org.apache.geaflow.console.core.model.data.GeaflowEndpoint;
import org.apache.geaflow.console.core.model.data.GeaflowGraph;
import org.apache.geaflow.console.core.model.data.GeaflowVertex;
import org.apache.geaflow.console.core.model.plugin.config.GeaflowPluginConfig;
import org.springframework.stereotype.Component;

@Component
public class GraphConverter extends DataConverter<GeaflowGraph, GraphEntity> {

    @Override
    protected GraphEntity modelToEntity(GeaflowGraph model) {
        GraphEntity entity = super.modelToEntity(model);
        String configId = model.getPluginConfig().getId();
        entity.setPluginConfigId(configId);
        return entity;
    }

    public GeaflowGraph convert(GraphEntity entity, List<GeaflowVertex> vertices, List<GeaflowEdge> edges,
                                List<GeaflowEndpoint> endpoints, GeaflowPluginConfig pluginConfig) {
        GeaflowGraph graph = entityToModel(entity);
        graph.addVertices(vertices);
        graph.addEdges(edges);
        graph.setEndpoints(endpoints);
        graph.setPluginConfig(pluginConfig);
        return graph;
    }
}
