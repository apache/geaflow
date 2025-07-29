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

package org.apache.geaflow.dsl.catalog.console;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.calcite.schema.Table;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.dsl.catalog.Catalog;
import org.apache.geaflow.dsl.catalog.exception.ObjectAlreadyExistException;
import org.apache.geaflow.dsl.common.descriptor.EdgeDescriptor;
import org.apache.geaflow.dsl.schema.GeaFlowFunction;
import org.apache.geaflow.dsl.schema.GeaFlowGraph;
import org.apache.geaflow.dsl.schema.GeaFlowGraph.EdgeTable;
import org.apache.geaflow.dsl.schema.GeaFlowGraph.VertexTable;
import org.apache.geaflow.dsl.schema.GeaFlowTable;
import org.apache.geaflow.dsl.schema.GeaFlowView;

public class ConsoleCatalog implements Catalog {

    public static final String CATALOG_TYPE = "console";

    private final Map<String, Map<String, Table>> allTables = new HashMap<>();

    private final Map<String, Map<String, GeaFlowFunction>> allFunctions = new HashMap<>();

    private Set<String> allInstances;

    private Set<String> allGraphsAndTables;

    private ConsoleCatalogClient client;

    @Override
    public void init(Configuration config) {
        this.client = new ConsoleCatalogClient(config);
    }

    @Override
    public String getType() {
        return CATALOG_TYPE;
    }

    @Override
    public Set<String> listInstances() {
        if (allInstances == null) {
            allInstances = client.getInstances();
        }
        return allInstances;
    }

    @Override
    public boolean isInstanceExists(String instanceName) {
        Set<String> allInstances = listInstances();
        return allInstances.contains(instanceName);
    }

    @Override
    public Table getGraph(String instanceName, String graphName) {
        return allTables
            .computeIfAbsent(instanceName, k -> new HashMap<>())
            .computeIfAbsent(graphName, k -> client.getGraph(instanceName, graphName));
    }

    @Override
    public Table getTable(String instanceName, String tableName) {
        return allTables
            .computeIfAbsent(instanceName, k -> new HashMap<>())
            .computeIfAbsent(tableName, k -> client.getTable(instanceName, tableName));
    }

    @Override
    public VertexTable getVertex(String instanceName, String vertexName) {
        return (VertexTable) allTables
            .computeIfAbsent(instanceName, k -> new HashMap<>())
            .computeIfAbsent(vertexName, k -> client.getVertex(instanceName, vertexName));
    }

    @Override
    public EdgeTable getEdge(String instanceName, String edgeName) {
        return (EdgeTable) allTables
            .computeIfAbsent(instanceName, k -> new HashMap<>())
            .computeIfAbsent(edgeName, k -> client.getEdge(instanceName, edgeName));
    }

    @Override
    public GeaFlowFunction getFunction(String instanceName, String functionName) {
        return allFunctions
            .computeIfAbsent(instanceName, k -> new HashMap<>())
            .computeIfAbsent(functionName, k -> client.getFunction(instanceName, functionName));
    }

    @Override
    public Set<String> listGraphAndTable(String instanceName) {
        if (allGraphsAndTables == null) {
            allGraphsAndTables = new HashSet<>();
            allGraphsAndTables.addAll(client.getGraphs(instanceName));
            allGraphsAndTables.addAll(client.getTables(instanceName));
        }
        return allGraphsAndTables;
    }

    @Override
    public void createGraph(String instanceName, GeaFlowGraph graph)
        throws ObjectAlreadyExistException {
        if (getGraph(instanceName, graph.getName()) != null) {
            if (!graph.isIfNotExists()) {
                throw new ObjectAlreadyExistException(graph.getName());
            }
            return;
        }
        client.createGraph(instanceName, graph);
        Map<String, List<String>> edgeType2SourceTypes = new HashMap<>();
        Map<String, List<String>> edgeType2TargetTypes = new HashMap<>();
        for (EdgeDescriptor edgeDescriptor : graph.getDescriptor().edges) {
            edgeType2SourceTypes.computeIfAbsent(edgeDescriptor.type, t -> new ArrayList<>());
            edgeType2TargetTypes.computeIfAbsent(edgeDescriptor.type, t -> new ArrayList<>());
            edgeType2SourceTypes.get(edgeDescriptor.type).add(edgeDescriptor.sourceType);
            edgeType2TargetTypes.get(edgeDescriptor.type).add(edgeDescriptor.targetType);
        }
        List<String> edgeTypes = new ArrayList<>();
        List<String> sourceVertexTypes = new ArrayList<>();
        List<String> targetVertexTypes = new ArrayList<>();
        for (String edgeType : edgeType2SourceTypes.keySet()) {
            for (int i = 0; i < edgeType2SourceTypes.get(edgeType).size(); i++) {
                edgeTypes.add(edgeType);
                sourceVertexTypes.add(edgeType2SourceTypes.get(edgeType).get(i));
                targetVertexTypes.add(edgeType2TargetTypes.get(edgeType).get(i));
            }
        }
        client.createEdgeEndpoints(instanceName, graph.getName(), edgeTypes,
            sourceVertexTypes, targetVertexTypes);
        allTables.get(instanceName).put(graph.getName(), graph);
    }

    @Override
    public void createTable(String instanceName, GeaFlowTable table)
        throws ObjectAlreadyExistException {
        if (getTable(instanceName, table.getName()) != null) {
            if (!table.isIfNotExists()) {
                throw new ObjectAlreadyExistException(table.getName());
            }
            // ignore if table exists.
            return;
        }
        client.createTable(instanceName, table);
        allTables.get(instanceName).put(table.getName(), table);
    }

    @Override
    public void createView(String instanceName, GeaFlowView view)
        throws ObjectAlreadyExistException {
        Map<String, Table> tableMap = allTables
            .computeIfAbsent(instanceName, k -> new HashMap<>());
        Table geaFlowView = tableMap.get(view.getName());
        if (geaFlowView != null) {
            if (!view.isIfNotExists()) {
                throw new ObjectAlreadyExistException(view.getName());
            }
            return;
        }
        tableMap.put(view.getName(), view);
    }

    @Override
    public void createFunction(String instanceName, GeaFlowFunction function)
        throws ObjectAlreadyExistException {

    }

    @Override
    public void dropGraph(String instanceName, String graphName) {
        client.deleteGraph(instanceName, graphName);
        allTables.computeIfAbsent(instanceName, k -> new HashMap<>()).remove(graphName);
    }

    @Override
    public void dropTable(String instanceName, String tableName) {
        client.deleteTable(instanceName, tableName);
        allTables.computeIfAbsent(instanceName, k -> new HashMap<>()).remove(tableName);
    }

    @Override
    public void dropFunction(String instanceName, String functionName) {

    }

    @Override
    public String describeGraph(String instanceName, String graphName) {
        Table graph = getGraph(instanceName, graphName);
        if (graph != null) {
            return graph.toString();
        }
        return null;
    }

    @Override
    public String describeTable(String instanceName, String tableName) {
        Table table = getTable(instanceName, tableName);
        if (table != null) {
            return table.toString();
        }
        return null;
    }

    @Override
    public String describeFunction(String instanceName, String functionName) {
        return null;
    }
}
