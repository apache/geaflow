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
import java.util.Objects;
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

public class MemoryCatalog implements Catalog {

    public static final String CATALOG_TYPE = "memory";

    private final Map<String, Map<String, Table>> allTables = new HashMap<>();

    public MemoryCatalog() {
        allTables.put(Catalog.DEFAULT_INSTANCE, new HashMap<>());
    }

    @Override
    public void init(Configuration config) {

    }

    @Override
    public String getType() {
        return CATALOG_TYPE;
    }

    @Override
    public Set<String> listInstances() {
        Set<String> instances = new HashSet<>(allTables.keySet());
        return instances;
    }

    @Override
    public boolean isInstanceExists(String instanceName) {
        return Objects.equals(Catalog.DEFAULT_INSTANCE, instanceName)
            || allTables.containsKey(instanceName);
    }

    @Override
    public Table getGraph(String instanceName, String graphName) {
        Map<String, Table> graphs = allTables.get(instanceName);
        if (graphs != null) {
            return graphs.get(graphName);
        }
        return null;
    }

    @Override
    public Table getTable(String instanceName, String tableName) {
        Map<String, Table> tables = allTables.get(instanceName);
        if (tables != null) {
            return tables.get(tableName);
        }
        return null;
    }

    @Override
    public VertexTable getVertex(String instanceName, String vertexName) {
        Map<String, Table> tables = allTables.get(instanceName);
        if (tables != null && (tables.get(vertexName) instanceof VertexTable)) {
            return (VertexTable) tables.get(vertexName);
        }
        return null;
    }

    @Override
    public EdgeTable getEdge(String instanceName, String edgeName) {
        Map<String, Table> tables = allTables.get(instanceName);
        if (tables != null && (tables.get(edgeName) instanceof EdgeTable)) {
            return (EdgeTable) tables.get(edgeName);
        }
        return null;
    }

    @Override
    public GeaFlowFunction getFunction(String instanceName, String functionName) {
        return null;
    }

    @Override
    public Set<String> listGraphAndTable(String instanceName) {
        return new HashSet<>(allTables.get(instanceName).keySet());
    }

    @Override
    public void createGraph(String instanceName, GeaFlowGraph graph) throws ObjectAlreadyExistException {
        if (getGraph(instanceName, graph.getName()) != null) {
            if (!graph.isIfNotExists()) {
                throw new ObjectAlreadyExistException(graph.getName());
            }
            return;
        }
        allTables.computeIfAbsent(instanceName, k -> new HashMap<>()).put(graph.getName(), graph);
    }

    @Override
    public void createTable(String instanceName, GeaFlowTable table) throws ObjectAlreadyExistException {
        if (getTable(instanceName, table.getName()) != null) {
            if (!table.isIfNotExists()) {
                throw new ObjectAlreadyExistException(table.getName());
            }
            // ignore if table exists.
            return;
        }
        allTables.computeIfAbsent(instanceName, k -> new HashMap<>()).put(table.getName(), table);
    }

    @Override
    public void createView(String instanceName, GeaFlowView view) throws ObjectAlreadyExistException {
        if (getTable(instanceName, view.getName()) != null) {
            if (!view.isIfNotExists()) {
                throw new ObjectAlreadyExistException(view.getName());
            }
            return;
        }
        allTables.computeIfAbsent(instanceName, k -> new HashMap<>()).put(view.getName(), view);
    }

    @Override
    public void createFunction(String instanceName, GeaFlowFunction function) throws ObjectAlreadyExistException {

    }

    @Override
    public void dropGraph(String instanceName, String graphName) {
        Map<String, Table> graphs = allTables.get(instanceName);
        if (graphs == null) {
            throw new ObjectNotExistException(instanceName);
        }
        GeaFlowGraph graph = (GeaFlowGraph) graphs.get(graphName);
        if (graph == null) {
            throw new ObjectNotExistException(graphName);
        }
        graphs.remove(graphName);
    }

    @Override
    public void dropTable(String instanceName, String tableName) {
        Map<String, Table> tables = allTables.get(instanceName);
        if (tables == null) {
            throw new ObjectNotExistException(instanceName);
        }
        GeaFlowTable table = (GeaFlowTable) tables.get(tableName);
        if (table == null) {
            throw new ObjectNotExistException(tableName);
        }
        tables.remove(tableName);
    }

    @Override
    public void dropFunction(String instanceName, String functionName) {

    }

    @Override
    public String describeGraph(String instanceName, String graphName) {
        Map<String, Table> graphs = allTables.get(instanceName);
        if (graphs == null) {
            throw new ObjectNotExistException(instanceName);
        }
        GeaFlowGraph graph = (GeaFlowGraph) graphs.get(graphName);
        if (graph == null) {
            throw new ObjectNotExistException(graphName);
        }
        return graph.toString();
    }

    @Override
    public String describeTable(String instanceName, String tableName) {
        Map<String, Table> tables = allTables.get(instanceName);
        if (tables == null) {
            throw new ObjectNotExistException(instanceName);
        }
        GeaFlowTable table = (GeaFlowTable) tables.get(tableName);
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
