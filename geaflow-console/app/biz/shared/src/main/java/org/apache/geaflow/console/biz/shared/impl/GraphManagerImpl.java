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

package org.apache.geaflow.console.biz.shared.impl;

import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.commons.collections.CollectionUtils;
import org.apache.geaflow.console.biz.shared.EdgeManager;
import org.apache.geaflow.console.biz.shared.GraphManager;
import org.apache.geaflow.console.biz.shared.PluginConfigManager;
import org.apache.geaflow.console.biz.shared.VertexManager;
import org.apache.geaflow.console.biz.shared.convert.DataViewConverter;
import org.apache.geaflow.console.biz.shared.convert.GraphViewConverter;
import org.apache.geaflow.console.biz.shared.convert.PluginConfigViewConverter;
import org.apache.geaflow.console.biz.shared.view.EndpointView;
import org.apache.geaflow.console.biz.shared.view.GraphView;
import org.apache.geaflow.console.biz.shared.view.IdView;
import org.apache.geaflow.console.biz.shared.view.PluginConfigView;
import org.apache.geaflow.console.common.dal.entity.GraphEntity;
import org.apache.geaflow.console.common.dal.model.GraphSearch;
import org.apache.geaflow.console.common.util.Fmt;
import org.apache.geaflow.console.common.util.ListUtil;
import org.apache.geaflow.console.common.util.type.GeaflowPluginCategory;
import org.apache.geaflow.console.common.util.type.GeaflowStructType;
import org.apache.geaflow.console.core.model.data.GeaflowEdge;
import org.apache.geaflow.console.core.model.data.GeaflowEndpoint;
import org.apache.geaflow.console.core.model.data.GeaflowGraph;
import org.apache.geaflow.console.core.model.data.GeaflowVertex;
import org.apache.geaflow.console.core.model.plugin.config.GeaflowPluginConfig;
import org.apache.geaflow.console.core.service.DataService;
import org.apache.geaflow.console.core.service.EdgeService;
import org.apache.geaflow.console.core.service.GraphService;
import org.apache.geaflow.console.core.service.PluginConfigService;
import org.apache.geaflow.console.core.service.VertexService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class GraphManagerImpl extends DataManagerImpl<GeaflowGraph, GraphView, GraphSearch> implements GraphManager {

    @Autowired
    private GraphService graphService;

    @Autowired
    private VertexService vertexService;

    @Autowired
    private VertexManager vertexManager;

    @Autowired
    private EdgeService edgeService;

    @Autowired
    private EdgeManager edgeManager;

    @Autowired
    private PluginConfigManager pluginConfigManager;

    @Autowired
    private PluginConfigService pluginConfigService;

    @Autowired
    private GraphViewConverter graphViewConverter;

    @Autowired
    private PluginConfigViewConverter pluginConfigViewConverter;

    @Override
    public DataViewConverter<GeaflowGraph, GraphView> getConverter() {
        return graphViewConverter;
    }

    @Override
    public DataService<GeaflowGraph, GraphEntity, GraphSearch> getService() {
        return graphService;
    }

    @Override
    public List<String> create(String instanceName, List<GraphView> views) {
        for (GraphView g : views) {
            g.setVertices(Optional.ofNullable(g.getVertices()).orElse(new ArrayList<>()));
            g.setEdges(Optional.ofNullable(g.getEdges()).orElse(new ArrayList<>()));

            // create if id is null
            vertexManager.createIfIdAbsent(instanceName, g.getVertices());
            edgeManager.createIfIdAbsent(instanceName, g.getEdges());

            PluginConfigView pluginConfigView = Preconditions.checkNotNull(g.getPluginConfig(),
                "Graph pluginConfig is required");
            pluginConfigView.setCategory(GeaflowPluginCategory.GRAPH);
            pluginConfigView.setName(Fmt.as("{}-{}-graph-config", instanceName, g.getName()));
            pluginConfigManager.create(pluginConfigView);
        }

        return super.create(instanceName, views);
    }

    @Override
    @Transactional
    public boolean updateByName(String instanceName, String name, GraphView view) {
        // only support to add vertices and edges
        vertexManager.createIfIdAbsent(instanceName, view.getVertices());
        edgeManager.createIfIdAbsent(instanceName, view.getEdges());

        return super.updateByName(instanceName, name, view);
    }


    @Override
    protected List<GeaflowGraph> parse(List<GraphView> views) {
        return ListUtil.convert(views, g -> {
            List<String> vertexIds = ListUtil.convert(g.getVertices(), IdView::getId);
            List<String> edgeIds = ListUtil.convert(g.getEdges(), IdView::getId);
            Map<String, Integer> orderMap = getOrderMap(vertexIds, edgeIds);

            // ensure the order of vertices and edges
            List<GeaflowVertex> vertices = vertexService.get(vertexIds);
            vertices = vertices.stream()
                .sorted(Comparator.comparing(e -> {
                    String key = getResourceKey(e.getType(), e.getId());
                    return orderMap.get(key);
                })).collect(Collectors.toList());

            List<GeaflowEdge> edges = edgeService.get(edgeIds);
            edges = edges.stream()
                .sorted(Comparator.comparing(e -> {
                    String key = getResourceKey(e.getType(), e.getId());
                    return orderMap.get(key);
                })).collect(Collectors.toList());

            GeaflowPluginConfig pluginConfig = pluginConfigViewConverter.convert(g.getPluginConfig());

            return graphViewConverter.convert(g, vertices, edges, pluginConfig);
        });
    }

    private Map<String, Integer> getOrderMap(List<String> vertexIds, List<String> edgeIds) {
        Map<String, Integer> orderMap = new HashMap<>();
        for (int i = 0; i < vertexIds.size(); i++) {
            orderMap.put(getResourceKey(GeaflowStructType.VERTEX, vertexIds.get(i)), i);
        }

        for (int i = 0; i < edgeIds.size(); i++) {
            orderMap.put(getResourceKey(GeaflowStructType.EDGE, edgeIds.get(i)), i);
        }

        return orderMap;
    }

    private String getResourceKey(GeaflowStructType structType, String resourceId) {
        return structType.name() + "-" + resourceId;
    }

    @Override
    @Transactional
    public boolean createEndpoints(String instanceName, String graphName, List<EndpointView> views) {
        if (CollectionUtils.isEmpty(views)) {
            return true;
        }

        String instanceId = getInstanceIdByName(instanceName);
        GeaflowGraph graph = graphService.getByName(instanceId, graphName);
        Preconditions.checkNotNull(graph, "Graph %s not exist", graphName);

        List<GeaflowEndpoint> endpoints = buildEndpoints(instanceId, views);
        return graphService.createEndpoints(graph, endpoints);
    }

    @Override
    public boolean deleteEndpoints(String instanceName, String edgeName, List<EndpointView> views) {
        String instanceId = getInstanceIdByName(instanceName);
        GeaflowGraph graph = graphService.getByName(instanceId, edgeName);
        Preconditions.checkNotNull(graph, "Graph %s not exist", edgeName);
        List<GeaflowEndpoint> endpoints = buildEndpoints(instanceId, views);
        return graphService.deleteEndpoints(graph, endpoints);
    }

    @Override
    public boolean clean(String instanceName, String graphName) {
        String instanceId = getInstanceIdByName(instanceName);
        GeaflowGraph graph = graphService.getByName(instanceId, graphName);
        return graphService.clean(Collections.singletonList(graph));
    }

    private List<GeaflowEndpoint> buildEndpoints(String instanceId, List<EndpointView> endpointViews) {
        return ListUtil.convert(endpointViews, e -> {
            // check vertex/edge existing and build endpoint models
            String edgeName = e.getEdgeName();
            String srcName = e.getSourceName();
            String targetName = e.getTargetName();
            GeaflowEdge edge = edgeService.getByName(instanceId, edgeName);
            Preconditions.checkNotNull(edge, "Edge %s not exist", edgeName);
            GeaflowVertex srcVertex = vertexService.getByName(instanceId, srcName);
            Preconditions.checkNotNull(srcVertex, "Vertex %s not exist", srcName);
            GeaflowVertex targetVertex = vertexService.getByName(instanceId, targetName);
            Preconditions.checkNotNull(targetVertex, "Vertex %s not exist", targetName);
            return new GeaflowEndpoint(edge.getId(), srcVertex.getId(), targetVertex.getId());
        });
    }
}
