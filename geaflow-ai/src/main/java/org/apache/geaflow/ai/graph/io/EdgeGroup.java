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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class EdgeGroup implements EntityGroup {

    public final EdgeSchema edgeSchema;
    private final List<Edge> edges;
    private final Map<String, Integer> index;
    private final Map<String, List<Integer>> pointIndices;

    public EdgeGroup(EdgeSchema edgeSchema, List<Edge> edges) {
        this.edgeSchema = edgeSchema;
        this.edges = edges;
        this.index = new HashMap<>(edges.size());
        this.pointIndices = new HashMap<>(10000);
        buildIndex();
    }

    private void buildIndex() {
        int index = 0;
        for (Edge e : edges) {
            String key = makeKey(e.getSrcId(), e.getDstId());
            this.index.put(key, index);
            this.pointIndices.computeIfAbsent(e.getSrcId(), k -> new ArrayList<>()).add(index);
            this.pointIndices.computeIfAbsent(e.getDstId(), k -> new ArrayList<>()).add(index);
            index++;
        }
    }

    public EdgeSchema getEdgeSchema() {
        return edgeSchema;
    }

    public List<Edge> getOutEdges() {
        return edges;
    }

    public List<Edge> getOutEdges(String src) {
        if (pointIndices.get(src) == null) {
            return new ArrayList<>();
        }
        return pointIndices.get(src).stream().map(edges::get)
                .filter(e -> e.getSrcId().equals(src))
                .collect(Collectors.toList());
    }

    public List<Edge> getInEdges(String src) {
        if (pointIndices.get(src) == null) {
            return new ArrayList<>();
        }
        return pointIndices.get(src).stream().map(edges::get)
                .filter(e -> e.getDstId().equals(src))
                .collect(Collectors.toList());
    }

    public Edge getEdge(String src, String dst) {
        if (index.get(makeKey(src, dst)) != null) {
            return edges.get(index.get(makeKey(src, dst)));
        } else {
            return null;
        }
    }

    private String makeKey(String src, String dst) {
        return src + "-" + dst;
    }
}
