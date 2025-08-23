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

package org.apache.geaflow.plan.graph;

import com.google.common.collect.Lists;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PipelineGraph implements Serializable {

    private static final Logger LOGGER = LoggerFactory.getLogger(PipelineGraph.class);

    private final Map<Integer, PipelineVertex> vertexMap;
    private final Map<Integer, PipelineEdge> edgeMap;
    private final Map<Integer, List<Integer>> vertexOutputEdgeIds;
    private final Map<Integer, List<Integer>> vertexInputEdgeIds;

    public PipelineGraph() {
        this.vertexMap = new HashMap<>();
        this.edgeMap = new LinkedHashMap<>();
        this.vertexOutputEdgeIds = new HashMap<>();
        this.vertexInputEdgeIds = new HashMap<>();
    }

    public void addVertex(PipelineVertex pipelineVertex) {
        LOGGER.info("add vertex:{} {}", pipelineVertex.getVertexId(), pipelineVertex.getName());
        this.vertexMap.put(pipelineVertex.getVertexId(), pipelineVertex);
    }

    public void setPipelineVertices(Set<PipelineVertex> pipelineVertices) {
        this.vertexMap.clear();
        pipelineVertices.stream().forEach(pipelineVertex -> addVertex(pipelineVertex));
    }

    public void addEdge(PipelineEdge pipelineEdge) {
        LOGGER.info("add edgeId:{}, edgeName:{}, srcId:{}, targetId:{}", pipelineEdge.getEdgeId(),
            pipelineEdge.getEdgeName(), pipelineEdge.getSrcId(), pipelineEdge.getTargetId());
        this.edgeMap.put(pipelineEdge.getEdgeId(), pipelineEdge);

        int edgeId = pipelineEdge.getEdgeId();
        int srcId = pipelineEdge.getSrcId();
        int tarId = pipelineEdge.getTargetId();
        if (vertexOutputEdgeIds.containsKey(srcId)) {
            vertexOutputEdgeIds.get(srcId).add(edgeId);
        } else {
            vertexOutputEdgeIds.put(srcId, Lists.newArrayList(edgeId));
        }

        if (vertexInputEdgeIds.containsKey(tarId)) {
            vertexInputEdgeIds.get(tarId).add(edgeId);
        } else {
            vertexInputEdgeIds.put(tarId, Lists.newArrayList(edgeId));
        }
    }

    public void setPipelineEdges(Set<PipelineEdge> pipelineEdges) {
        this.edgeMap.clear();
        this.vertexInputEdgeIds.clear();
        this.vertexOutputEdgeIds.clear();

        pipelineEdges.stream().forEach(pipelineEdge -> addEdge(pipelineEdge));
    }

    public Collection<PipelineEdge> getPipelineEdgeList() {
        return this.edgeMap.values();
    }

    public Map<Integer, PipelineVertex> getVertexMap() {
        return vertexMap;
    }

    public Collection<PipelineVertex> getPipelineVertices() {
        return vertexMap.values();
    }

    public Set<PipelineEdge> getVertexOutEdges(int vertexId) {
        return getVertexEdgesByIds(this.vertexOutputEdgeIds.get(vertexId));
    }

    public Set<PipelineEdge> getVertexInputEdges(int vertexId) {
        return getVertexEdgesByIds(this.vertexInputEdgeIds.get(vertexId));
    }

    private Set<PipelineEdge> getVertexEdgesByIds(List<Integer> edgeIds) {
        if (edgeIds == null) {
            return new HashSet<>();
        }
        LinkedHashSet<PipelineEdge> edges = new LinkedHashSet<>();
        edgeIds.stream().map(id -> edgeMap.get(id)).forEach(edges::add);
        return edges;
    }

    public Map<Integer, Set<PipelineEdge>> getVertexInputEdges() {
        Map<Integer, Set<PipelineEdge>> inputEdges = new HashMap<>(vertexMap.size());
        for (int key : vertexMap.keySet()) {
            inputEdges.put(key, new HashSet<>());
        }
        for (PipelineEdge executeEdge : getPipelineEdgeList()) {
            inputEdges.get(executeEdge.getTargetId()).add(executeEdge);

        }
        return inputEdges;
    }

    public Map<Integer, Set<PipelineEdge>> getVertexOutputEdges() {
        Map<Integer, Set<PipelineEdge>> outputEdges = new HashMap<>(vertexMap.size());
        for (int key : vertexMap.keySet()) {
            outputEdges.put(key, new HashSet<>());
        }
        for (PipelineEdge executeEdge : getPipelineEdgeList()) {
            outputEdges.get(executeEdge.getSrcId()).add(executeEdge);
        }
        return outputEdges;
    }

    public List<PipelineVertex> getSourceVertices() {
        List<PipelineVertex> sourceVertices = new ArrayList<>();
        for (Map.Entry<Integer, PipelineVertex> entry : vertexMap.entrySet()) {
            if (entry.getValue().getType() == VertexType.source) {
                sourceVertices.add(entry.getValue());
            }
        }
        return sourceVertices;
    }

    public List<Integer> getVertexInputVertexIds(int vertexId) {
        if (vertexInputEdgeIds.get(vertexId) != null) {
            return vertexInputEdgeIds.get(vertexId).stream()
                .map(edgeId -> edgeMap.get(edgeId).getSrcId()).collect(Collectors.toList());
        } else {
            return new ArrayList<>();
        }
    }

    public List<Integer> getVertexOutputVertexIds(int vertexId) {
        if (vertexOutputEdgeIds.get(vertexId) != null) {
            return vertexOutputEdgeIds.get(vertexId).stream()
                .map(edgeId -> edgeMap.get(edgeId).getTargetId()).collect(Collectors.toList());
        } else {
            return new ArrayList<>();
        }
    }
}

