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

import java.util.Set;
import org.apache.calcite.schema.Table;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.dsl.catalog.exception.ObjectAlreadyExistException;
import org.apache.geaflow.dsl.schema.GeaFlowFunction;
import org.apache.geaflow.dsl.schema.GeaFlowGraph;
import org.apache.geaflow.dsl.schema.GeaFlowGraph.EdgeTable;
import org.apache.geaflow.dsl.schema.GeaFlowGraph.VertexTable;
import org.apache.geaflow.dsl.schema.GeaFlowTable;
import org.apache.geaflow.dsl.schema.GeaFlowView;

public interface Catalog {

    String DEFAULT_INSTANCE = "default";

    /**
     * Init catalog.
     *
     * @param config environment config.
     */
    void init(Configuration config);

    /**
     * Get the catalog type name.
     *
     * @return catalog type name.
     */
    String getType();

    /**
     * List all the instance names.
     *
     * @return The instance name list.
     */
    Set<String> listInstances();

    /**
     * Test if the instance name exists.
     *
     * @param instanceName The instance name.
     * @return true if exists, else return false.
     */
    boolean isInstanceExists(String instanceName);

    /**
     * Get graph of the instance and graph name.
     *
     * @param instanceName The instance name.
     * @param graphName    The graph name.
     * @return A {@link GeaFlowGraph}.
     */
    Table getGraph(String instanceName, String graphName);

    Table getTable(String instanceName, String tableName);

    VertexTable getVertex(String instanceName, String vertexName);

    EdgeTable getEdge(String instanceName, String edgeName);

    GeaFlowFunction getFunction(String instanceName, String functionName);

    Set<String> listGraphAndTable(String instanceName);

    /**
     * Create a graph under the specified instance.
     *
     * @param instanceName The instance name for the graph to create.
     * @param graph        The graph to create.
     * @throws ObjectAlreadyExistException Throwing {@link ObjectAlreadyExistException}
     *                                     when the graph has already exists.
     */
    void createGraph(String instanceName, GeaFlowGraph graph) throws ObjectAlreadyExistException;

    /**
     * Create a table under the specified instance.
     *
     * @param instanceName The instance name for the graph to create.
     * @param table        The table to create.
     * @throws ObjectAlreadyExistException Throwing {@link ObjectAlreadyExistException}
     *                                     when the table has already exists.
     */
    void createTable(String instanceName, GeaFlowTable table) throws ObjectAlreadyExistException;

    /**
     * Create a view under the specified instance.
     *
     * @param instanceName The instance name for the graph to create.
     * @param view         The view to create.
     * @throws ObjectAlreadyExistException Throwing {@link ObjectAlreadyExistException}
     *                                     when the view has already exists.
     */
    void createView(String instanceName, GeaFlowView view) throws ObjectAlreadyExistException;

    /**
     * Create a function under the specified instance.
     *
     * @param instanceName The instance name for the graph to create.
     * @param function     The function to create.
     * @throws ObjectAlreadyExistException Throwing {@link ObjectAlreadyExistException}
     *                                     when the function has already exists.
     */
    void createFunction(String instanceName, GeaFlowFunction function) throws ObjectAlreadyExistException;

    /**
     * Drop a graph under the specified instance.
     *
     * @param instanceName The instance name for the graph to drop.
     * @param graphName    The graph name to drop.
     */
    void dropGraph(String instanceName, String graphName);

    /**
     * Drop a table under the specified instance.
     *
     * @param instanceName The instance name for the table to drop.
     * @param tableName    The table name to drop.
     */
    void dropTable(String instanceName, String tableName);

    /**
     * Drop a function under the specified instance.
     *
     * @param instanceName The instance name for the function to drop.
     * @param functionName The function name to drop.
     */
    void dropFunction(String instanceName, String functionName);

    /**
     * Describe the graph information.
     *
     * @param instanceName The instance name for the graph.
     * @param graphName    The graph name to describe.
     * @return The information of the graph.
     */
    String describeGraph(String instanceName, String graphName);

    /**
     * Describe the table information.
     *
     * @param instanceName The instance name for the table.
     * @param tableName    The table name to describe.
     * @return The information of the table.
     */
    String describeTable(String instanceName, String tableName);

    /**
     * Describe the function information.
     *
     * @param instanceName The instance name for the function.
     * @param functionName The function name to describe.
     * @return The information of the function.
     */
    String describeFunction(String instanceName, String functionName);
}
