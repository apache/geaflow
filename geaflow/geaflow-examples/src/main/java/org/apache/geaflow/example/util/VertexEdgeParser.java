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

package org.apache.geaflow.example.util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.geaflow.common.tuple.Tuple;
import org.apache.geaflow.model.graph.edge.EdgeDirection;
import org.apache.geaflow.model.graph.edge.IEdge;
import org.apache.geaflow.model.graph.edge.impl.ValueEdge;
import org.apache.geaflow.model.graph.vertex.IVertex;
import org.apache.geaflow.model.graph.vertex.impl.ValueVertex;

public class VertexEdgeParser {

    public static List<IVertex<Integer, Map<String, String>>> vertexParserMap(String line) {
        Map<String, String> vertexValue = new HashMap<>(2);
        String[] split = line.split("\\s+");
        IVertex<Integer, Map<String, String>> v1 = new ValueVertex<>(Integer.parseInt(split[0]), vertexValue);
        IVertex<Integer, Map<String, String>> v2 = new ValueVertex<>(Integer.parseInt(split[1]), vertexValue);
        List<IVertex<Integer, Map<String, String>>> vertices = new ArrayList<>();
        vertices.add(v1);
        vertices.add(v2);
        return vertices;
    }

    public static List<IEdge<Integer, Map<String, Integer>>> edgeParserMap(String line) {
        Map<String, Integer> edgeValue = new HashMap<>(2);
        String[] split = line.split("\\s+");
        int src = Integer.parseInt(split[0]);
        int dst = Integer.parseInt(split[1]);
        edgeValue.put("dis", 1);
        IEdge<Integer, Map<String, Integer>> e1 = new ValueEdge<Integer, Map<String, Integer>>(src, dst, edgeValue, EdgeDirection.OUT);
        IEdge<Integer, Map<String, Integer>> e2 = new ValueEdge<Integer, Map<String, Integer>>(dst, src, edgeValue, EdgeDirection.IN);
        List<IEdge<Integer, Map<String, Integer>>> edges = new ArrayList<>();
        edges.add(e1);
        edges.add(e2);
        return edges;
    }

    public static List<IVertex<Integer, Map<String, Map<Integer, Object>>>> vertexParserMapMap(String line) {
        Map<String, Map<Integer, Object>> vertexValue = new HashMap<>(2);
        String[] split = line.split("\\s+");
        IVertex<Integer, Map<String, Map<Integer, Object>>> v1 = new ValueVertex<>(Integer.parseInt(split[0]), vertexValue);
        IVertex<Integer, Map<String, Map<Integer, Object>>> v2 = new ValueVertex<>(Integer.parseInt(split[1]), vertexValue);
        List<IVertex<Integer, Map<String, Map<Integer, Object>>>> vertices = new ArrayList<>();
        vertices.add(v1);
        vertices.add(v2);
        return vertices;
    }

    public static List<IVertex<Integer, Map<String, Object>>> vertexParserObjectMap(String line) {
        Map<String, Object> vertexValue = new HashMap<>(2);
        String[] split = line.split("\\s+");
        IVertex<Integer, Map<String, Object>> v1 = new ValueVertex<>(Integer.parseInt(split[0]), vertexValue);
        IVertex<Integer, Map<String, Object>> v2 = new ValueVertex<>(Integer.parseInt(split[1]), vertexValue);
        List<IVertex<Integer, Map<String, Object>>> vertices = new ArrayList<>();
        vertices.add(v1);
        vertices.add(v2);
        return vertices;
    }


    public static List<IVertex<Integer, Integer>> vertexParserInteger(String line) {
        String[] split = line.split("\\s+");
        IVertex<Integer, Integer> v1 = new ValueVertex<>(Integer.parseInt(split[0]), 0);
        IVertex<Integer, Integer> v2 = new ValueVertex<>(Integer.parseInt(split[1]), 0);
        List<IVertex<Integer, Integer>> vertices = new ArrayList<>();
        vertices.add(v1);
        vertices.add(v2);
        return vertices;
    }

    public static List<IEdge<Integer, Integer>> edgeParserInteger(String line) {
        String[] split = line.split("\\s+");
        int src = Integer.parseInt(split[0]);
        int dst = Integer.parseInt(split[1]);
        IEdge<Integer, Integer> e1 = new ValueEdge<>(src, dst, 0, EdgeDirection.OUT);
        IEdge<Integer, Integer> e2 = new ValueEdge<>(dst, src, 0, EdgeDirection.IN);
        List<IEdge<Integer, Integer>> edges = new ArrayList<>();
        edges.add(e1);
        edges.add(e2);
        return edges;
    }

    public static List<IVertex<Integer, Tuple<Double, Integer>>> vertexParserTuple(String line) {
        String[] split = line.split("\\s+");
        IVertex<Integer, Tuple<Double, Integer>> v1 = new ValueVertex<>(Integer.parseInt(split[0]), Tuple.of(0.0, 0));
        IVertex<Integer, Tuple<Double, Integer>> v2 = new ValueVertex<>(Integer.parseInt(split[1]), Tuple.of(0.0, 0));
        List<IVertex<Integer, Tuple<Double, Integer>>> vertices = new ArrayList<>();
        vertices.add(v1);
        vertices.add(v2);
        return vertices;
    }

    public static List<IVertex<Integer, Double>> vertexParserDouble(String line) {
        String[] split = line.split("\\s+");
        IVertex<Integer, Double> v1 = new ValueVertex<>(Integer.parseInt(split[0]), 0.1);
        IVertex<Integer, Double> v2 = new ValueVertex<>(Integer.parseInt(split[1]), 0.1);
        List<IVertex<Integer, Double>> vertices = new ArrayList<>();
        vertices.add(v1);
        vertices.add(v2);
        return vertices;
    }

    public static List<IVertex<Integer, Boolean>> vertexParserBoolean(String line) {
        String[] split = line.split("\\s+");
        IVertex<Integer, Boolean> v1 = new ValueVertex<>(Integer.parseInt(split[0]), false);
        IVertex<Integer, Boolean> v2 = new ValueVertex<>(Integer.parseInt(split[1]), false);
        List<IVertex<Integer, Boolean>> vertices = new ArrayList<>();
        vertices.add(v1);
        vertices.add(v2);
        return vertices;
    }

    public static List<IEdge<Integer, Boolean>> edgeParserBoolean(String line) {
        String[] split = line.split("\\s+");
        int src = Integer.parseInt(split[0]);
        int dst = Integer.parseInt(split[1]);
        IEdge<Integer, Boolean> e1 = new ValueEdge<>(src, dst, true, EdgeDirection.OUT);
        IEdge<Integer, Boolean> e2 = new ValueEdge<>(dst, src, true, EdgeDirection.IN);
        List<IEdge<Integer, Boolean>> edges = new ArrayList<>();
        edges.add(e1);
        edges.add(e2);
        return edges;
    }

}
