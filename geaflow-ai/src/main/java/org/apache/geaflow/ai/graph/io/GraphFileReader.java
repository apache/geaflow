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

import java.io.IOException;
import java.util.*;

public class GraphFileReader {

    public static Graph getGraph(ClassLoader classLoader, String path, long limit) throws IOException {
        Map<String, List<String>> result = ResourceFileScanner.scanGraphLdbcSfFolder(classLoader, path);

        Set<String> vertexNames = new HashSet<>(Arrays.asList(
                "Comment","Forum","Organisation","Person","Place","Post","Tag","TagClass"
        ));

        GraphSchema graphSchema = new GraphSchema();
        Map<String, EntityGroup> entities = new HashMap<>();
        for (Map.Entry<String, List<String>> entry : result.entrySet()) {
            String entityName = entry.getKey();
            boolean isVertex = vertexNames.contains(entityName);
            List<String> fileNames = entry.getValue();
            for (String fileName : fileNames) {
                CsvFileReader reader = new CsvFileReader(limit);
                reader.readCsvFile(path + "/" + entityName + "/" + fileName);
                List<String> colSchema = reader.getColSchema();
                if (isVertex) {
                    int idIndex = colSchema.indexOf("id");
                    if (idIndex < 0) {
                        throw new RuntimeException("找不到Id的索引下标");
                    }
                    VertexSchema vertexSchema = new VertexSchema(entityName, colSchema.get(idIndex), colSchema);
                    List<Vertex> vertices = new ArrayList<>(reader.getRowCount());
                    for (int i = 0; i < reader.getRowCount(); i++) {
                        List<String> row = reader.getRow(i);
                        vertices.add(new Vertex(entityName, row.get(idIndex), row));
                    }
                    VertexGroup vertexGroup = new VertexGroup(vertexSchema, vertices);
                    entities.put(entityName, vertexGroup);
                    graphSchema.addVertex(vertexSchema);
                } else {
                    boolean containTime = colSchema.contains("creationDate");
                    int srcIdIndex = containTime ? 1 : 0;
                    int dstIdIndex = containTime ? 2 : 1;
                    EdgeSchema edgeSchema = new EdgeSchema(entityName, colSchema.get(srcIdIndex), colSchema.get(dstIdIndex), colSchema);
                    List<Edge> edges = new ArrayList<>(reader.getRowCount());
                    for (int i = 0; i < reader.getRowCount(); i++) {
                        List<String> row = reader.getRow(i);
                        edges.add(new Edge(entityName, row.get(srcIdIndex), row.get(dstIdIndex), row));
                    }
                    EdgeGroup edgeGroup = new EdgeGroup(edgeSchema, edges);
                    entities.put(entityName, edgeGroup);
                    graphSchema.addEdge(edgeSchema);
                }
            }
        }
        Graph ldbcGraph =  new Graph(graphSchema, entities);
        return ldbcGraph;
    }

}
