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

package org.apache.geaflow.dsl.catalog;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.calcite.schema.Table;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.dsl.catalog.exception.ObjectAlreadyExistException;
import org.apache.geaflow.dsl.catalog.exception.ObjectNotExistException;
import org.apache.geaflow.dsl.schema.GeaFlowFunction;
import org.apache.geaflow.dsl.schema.GeaFlowGraph;
import org.apache.geaflow.dsl.schema.GeaFlowGraph.EdgeTable;
import org.apache.geaflow.dsl.schema.GeaFlowGraph.VertexTable;
import org.apache.geaflow.dsl.schema.GeaFlowTable;
import org.apache.geaflow.dsl.schema.GeaFlowView;

public class CompileCatalog implements Catalog {

    private final Catalog catalog;

    private final Map<String, Map<String, Table>> allTables = new HashMap<>();

    public CompileCatalog(Catalog catalog) {
        this.catalog = catalog;
    }

    @Override
    public void init(Configuration config) {

    }

    @Override
    public String getType() {
        return catalog.getType();
    }

    @Override
    public Set<String> listInstances() {
        Set<String> allInstances = new HashSet<>(allTables.keySet());
        allInstances.addAll(catalog.listInstances());
        return allInstances;
    }

    @Override
    public boolean isInstanceExists(String instanceName) {
        return allTables.containsKey(instanceName) || catalog.isInstanceExists(instanceName);
    }

    @Override
    public Table getGraph(String instanceName, String graphName) {
        Table graph = allTables.computeIfAbsent(instanceName, k -> new HashMap<>()).get(graphName);
        if (graph != null) {
            return graph;
        }
        return catalog.getGraph(instanceName, graphName);
    }

    @Override
    public Table getTable(String instanceName, String tableName) {
        Table table = allTables.computeIfAbsent(instanceName, k -> new HashMap<>()).get(tableName);
        if (table != null) {
            return table;
        }
        return catalog.getTable(instanceName, tableName);
    }

    @Override
    public VertexTable getVertex(String instanceName, String vertexName) {
        Table table = allTables.computeIfAbsent(instanceName, k -> new HashMap<>()).get(vertexName);
        if (table instanceof VertexTable) {
            return (VertexTable) table;
        }
        return catalog.getVertex(instanceName, vertexName);
    }

    @Override
    public EdgeTable getEdge(String instanceName, String edgeName) {
        Table table = allTables.computeIfAbsent(instanceName, k -> new HashMap<>()).get(edgeName);
        if (table instanceof EdgeTable) {
            return (EdgeTable) table;
        }
        return catalog.getEdge(instanceName, edgeName);
    }

    @Override
    public GeaFlowFunction getFunction(String instanceName, String functionName) {
        return catalog.getFunction(instanceName, functionName);
    }

    @Override
    public Set<String> listGraphAndTable(String instanceName) {
        Set<String> graphAndTables = new HashSet<>();
        Map<String, Table> tables = allTables.get(instanceName);
        if (tables != null) {
            graphAndTables.addAll(tables.keySet());
        }
        graphAndTables.addAll(catalog.listGraphAndTable(instanceName));
        return graphAndTables;
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
        if (!isInstanceExists(instanceName)) {
            throw new ObjectNotExistException("instance: '" + instanceName + "' is not exists.");
        }
        allTables.computeIfAbsent(instanceName, k -> new HashMap<>()).put(graph.getName(), graph);
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
        if (!isInstanceExists(instanceName)) {
            throw new ObjectNotExistException("instance: '" + instanceName + "' is not exists.");
        }
        allTables.computeIfAbsent(instanceName, k -> new HashMap<>()).put(table.getName(), table);
    }

    @Override
    public void createView(String instanceName, GeaFlowView view)
        throws ObjectAlreadyExistException {
        if (getTable(instanceName, view.getName()) != null) {
            if (!view.isIfNotExists()) {
                throw new ObjectAlreadyExistException(view.getName());
            }
            return;
        }
        if (!isInstanceExists(instanceName)) {
            throw new ObjectNotExistException("instance: '" + instanceName + "' is not exists.");
        }
        allTables.computeIfAbsent(instanceName, k -> new HashMap<>()).put(view.getName(), view);
    }

    @Override
    public void createFunction(String instanceName, GeaFlowFunction function)
        throws ObjectAlreadyExistException {

    }

    @Override
    public void dropGraph(String instanceName, String graphName) {
        Table graph = allTables.computeIfAbsent(instanceName, k -> new HashMap<>()).get(graphName);
        if (graph != null) {
            allTables.get(instanceName).remove(graphName);
            return;
        }
        Table graphFromCatalog = catalog.getGraph(instanceName, graphName);
        if (graphFromCatalog == null) {
            throw new ObjectNotExistException(graphName);
        }
    }

    @Override
    public void dropTable(String instanceName, String tableName) {
        Table table = allTables.computeIfAbsent(instanceName, k -> new HashMap<>()).get(tableName);
        if (table != null) {
            allTables.get(instanceName).remove(tableName);
            return;
        }
        Table tableFromCatalog = catalog.getTable(instanceName, tableName);
        if (tableFromCatalog == null) {
            throw new ObjectNotExistException(tableName);
        }
    }

    @Override
    public void dropFunction(String instanceName, String functionName) {

    }

    @Override
    public String describeGraph(String instanceName, String graphName) {
        Table graph = getGraph(instanceName, graphName);
        if (graph == null) {
            throw new ObjectNotExistException(graphName);
        }
        return graph.toString();
    }

    @Override
    public String describeTable(String instanceName, String tableName) {
        Table table = getTable(instanceName, tableName);
        if (table == null) {
            throw new ObjectNotExistException(tableName);
        }
        return table.toString();
    }

    @Override
    public String describeFunction(String instanceName, String functionName) {
        return null;
    }
}
