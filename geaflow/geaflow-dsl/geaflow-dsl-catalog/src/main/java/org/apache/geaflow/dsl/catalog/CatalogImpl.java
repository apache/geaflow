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

public class CatalogImpl implements Catalog {

    private final Catalog baseCatalog;

    private final Catalog memoryCatalog;

    private final Map<String, Map<String, Table>> tmpTables = new HashMap<>();

    public CatalogImpl(Catalog catalog) {
        this.baseCatalog = catalog;
        this.memoryCatalog = new MemoryCatalog();
    }

    @Override
    public void init(Configuration config) {
        memoryCatalog.init(config);
        baseCatalog.init(config);
    }

    @Override
    public String getType() {
        return baseCatalog.getType();
    }

    @Override
    public Set<String> listInstances() {
        Set<String> allInstances = new HashSet<>(memoryCatalog.listInstances());
        allInstances.addAll(baseCatalog.listInstances());
        return allInstances;
    }

    @Override
    public boolean isInstanceExists(String instanceName) {
        return memoryCatalog.isInstanceExists(instanceName) || baseCatalog.isInstanceExists(instanceName);
    }

    @Override
    public Table getGraph(String instanceName, String graphName) {
        Table graph = memoryCatalog.getGraph(instanceName, graphName);
        if (graph != null) {
            return graph;
        }
        return baseCatalog.getGraph(instanceName, graphName);
    }

    @Override
    public Table getTable(String instanceName, String tableName) {
        Table table = memoryCatalog.getTable(instanceName, tableName);
        if (table != null) {
            return table;
        }
        table = baseCatalog.getTable(instanceName, tableName);
        if (table != null) {
            return table;
        }
        table = getVertex(instanceName, tableName);
        if (table != null) {
            return table;
        }
        return getEdge(instanceName, tableName);
    }

    @Override
    public VertexTable getVertex(String instanceName, String vertexName) {
        Table table = memoryCatalog.getTable(instanceName, vertexName);
        if (table instanceof VertexTable) {
            return (VertexTable) table;
        }
        return baseCatalog.getVertex(instanceName, vertexName);
    }

    @Override
    public EdgeTable getEdge(String instanceName, String edgeName) {
        Table table = memoryCatalog.getTable(instanceName, edgeName);
        if (table instanceof EdgeTable) {
            return (EdgeTable) table;
        }
        return baseCatalog.getEdge(instanceName, edgeName);
    }

    @Override
    public GeaFlowFunction getFunction(String instanceName, String functionName) {
        return baseCatalog.getFunction(instanceName, functionName);
    }

    @Override
    public Set<String> listGraphAndTable(String instanceName) {
        Set<String> graphAndTables = new HashSet<>(memoryCatalog.listGraphAndTable(instanceName));
        graphAndTables.addAll(baseCatalog.listGraphAndTable(instanceName));
        return graphAndTables;
    }

    @Override
    public void createGraph(String instanceName, GeaFlowGraph graph)
        throws ObjectAlreadyExistException, ObjectNotExistException {
        if (getGraph(instanceName, graph.getName()) != null) {
            if (!graph.isIfNotExists()) {
                throw new ObjectAlreadyExistException(graph.getName());
            }
            return;
        }
        if (!isInstanceExists(instanceName)) {
            throw new ObjectNotExistException("instance: '" + instanceName + "' is not exists.");
        }
        // create graph
        if (graph.isTemporary()) {
            memoryCatalog.createGraph(instanceName, graph);
        } else {
            baseCatalog.createGraph(instanceName, graph);
        }
        // create vertex table & edge table in catalog
        for (VertexTable vertexTable : graph.getVertexTables()) {
            createTable(instanceName, vertexTable);
        }
        for (EdgeTable edgeTable : graph.getEdgeTables()) {
            createTable(instanceName, edgeTable);
        }
    }

    @Override
    public void createTable(String instanceName, GeaFlowTable table)
        throws ObjectAlreadyExistException, ObjectNotExistException {
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
        if (table.isTemporary()) {
            memoryCatalog.createTable(instanceName, table);
        } else {
            baseCatalog.createTable(instanceName, table);
        }
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
        baseCatalog.createView(instanceName, view);
    }

    @Override
    public void createFunction(String instanceName, GeaFlowFunction function)
        throws ObjectAlreadyExistException {
        baseCatalog.createFunction(instanceName, function);
    }

    @Override
    public void dropGraph(String instanceName, String graphName) {
        Table tmpGraph = memoryCatalog.getGraph(instanceName, graphName);
        if (tmpGraph != null) {
            memoryCatalog.dropGraph(instanceName, graphName);
        } else {
            Table graphFromCatalog = baseCatalog.getGraph(instanceName, graphName);
            if (graphFromCatalog == null) {
                throw new ObjectNotExistException(graphName);
            }
            baseCatalog.dropGraph(instanceName, graphName);
        }
    }

    @Override
    public void dropTable(String instanceName, String tableName) {
        Table table = memoryCatalog.getGraph(instanceName, tableName);
        if (table != null) {
            memoryCatalog.dropGraph(instanceName, tableName);
        } else {
            Table tableFromCatalog = baseCatalog.getTable(instanceName, tableName);
            if (tableFromCatalog == null) {
                throw new ObjectNotExistException(tableName);
            }
            baseCatalog.dropTable(instanceName, tableName);
        }

    }

    @Override
    public void dropFunction(String instanceName, String functionName) {
        baseCatalog.dropFunction(instanceName, functionName);
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
        return baseCatalog.describeFunction(instanceName, functionName);
    }
}
