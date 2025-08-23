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

package org.apache.geaflow.example.data;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import org.apache.geaflow.model.graph.edge.IEdge;
import org.apache.geaflow.model.graph.edge.impl.ValueEdge;
import org.apache.geaflow.model.graph.vertex.IVertex;
import org.apache.geaflow.model.graph.vertex.impl.ValueVertex;

public class GraphDataSet implements Serializable {

    public static final String DATASET_FILE = "data/input/web-google-mini";

    public static List<IVertex<Integer, Integer>> getIncVertices() {
        List<IVertex<Integer, Integer>> vertices = new ArrayList<>();
        vertices.add(new ValueVertex<>(1, 0));
        vertices.add(new ValueVertex<>(2, 0));
        vertices.add(new ValueVertex<>(3, 0));
        vertices.add(new ValueVertex<>(4, 0));
        vertices.add(new ValueVertex<>(5, 0));
        return vertices;
    }

    public static List<IEdge<Integer, Integer>> getIncEdges() {
        List<IEdge<Integer, Integer>> edges = new ArrayList<>();
        edges.add(new ValueEdge<>(1, 2, 12));
        edges.add(new ValueEdge<>(1, 3, 13));
        edges.add(new ValueEdge<>(1, 4, 13));
        edges.add(new ValueEdge<>(2, 4, 23));
        edges.add(new ValueEdge<>(2, 1, 23));
        edges.add(new ValueEdge<>(3, 5, 34));
        edges.add(new ValueEdge<>(4, 5, 35));
        edges.add(new ValueEdge<>(4, 2, 73));
        edges.add(new ValueEdge<>(2, 5, 45));
        edges.add(new ValueEdge<>(5, 1, 51));
        return edges;
    }

    public static List<IVertex<Integer, Double>> getPRVertices() {
        List<IVertex<Integer, Double>> vertices = new ArrayList<>();
        vertices.add(new ValueVertex<>(1, 0.2));
        vertices.add(new ValueVertex<>(2, 0.2));
        vertices.add(new ValueVertex<>(3, 0.2));
        vertices.add(new ValueVertex<>(4, 0.2));
        vertices.add(new ValueVertex<>(5, 0.2));
        return vertices;
    }

    public static List<IEdge<Integer, Integer>> getPREdges() {
        List<IEdge<Integer, Integer>> edges = new ArrayList<>();
        edges.add(new ValueEdge<>(1, 2, 12));
        edges.add(new ValueEdge<>(1, 3, 13));
        edges.add(new ValueEdge<>(1, 4, 13));
        edges.add(new ValueEdge<>(2, 4, 23));
        edges.add(new ValueEdge<>(3, 5, 34));
        edges.add(new ValueEdge<>(4, 5, 35));
        edges.add(new ValueEdge<>(2, 5, 45));
        edges.add(new ValueEdge<>(5, 1, 51));
        return edges;
    }

    public static List<IVertex<Integer, Integer>> getSSSPVertices() {
        List<IVertex<Integer, Integer>> vertices = new ArrayList<>();
        vertices.add(new ValueVertex<>(1, Integer.MAX_VALUE));
        vertices.add(new ValueVertex<>(2, Integer.MAX_VALUE));
        vertices.add(new ValueVertex<>(3, Integer.MAX_VALUE));
        vertices.add(new ValueVertex<>(4, Integer.MAX_VALUE));
        vertices.add(new ValueVertex<>(5, Integer.MAX_VALUE));
        vertices.add(new ValueVertex<>(6, Integer.MAX_VALUE));
        return vertices;
    }

    public static List<IEdge<Integer, Integer>> getSSSPEdges() {
        List<IEdge<Integer, Integer>> edges = new ArrayList<>();
        edges.add(new ValueEdge<>(1, 3, 10));
        edges.add(new ValueEdge<>(1, 5, 30));
        edges.add(new ValueEdge<>(1, 6, 100));
        edges.add(new ValueEdge<>(2, 3, 5));
        edges.add(new ValueEdge<>(3, 4, 50));
        edges.add(new ValueEdge<>(4, 6, 10));
        edges.add(new ValueEdge<>(5, 4, 20));
        edges.add(new ValueEdge<>(5, 6, 60));
        return edges;
    }

}
