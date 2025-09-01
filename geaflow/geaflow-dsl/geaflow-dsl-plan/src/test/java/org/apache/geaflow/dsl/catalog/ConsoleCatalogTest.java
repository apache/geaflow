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

import com.google.gson.Gson;
import java.io.IOException;
import java.util.Collections;
import java.util.Set;
import okhttp3.mockwebserver.Dispatcher;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.common.config.keys.DSLConfigKeys;
import org.apache.geaflow.common.config.keys.ExecutionConfigKeys;
import org.apache.geaflow.dsl.catalog.console.CatalogUtil;
import org.apache.geaflow.dsl.catalog.console.ConsoleCatalog;
import org.apache.geaflow.dsl.catalog.console.InstanceModel;
import org.apache.geaflow.dsl.catalog.console.PageList;
import org.apache.geaflow.dsl.common.descriptor.EdgeDescriptor;
import org.apache.geaflow.dsl.common.descriptor.GraphDescriptor;
import org.apache.geaflow.dsl.parser.GeaFlowDSLParser;
import org.apache.geaflow.dsl.planner.GQLContext;
import org.apache.geaflow.dsl.schema.GeaFlowGraph;
import org.apache.geaflow.dsl.schema.GeaFlowTable;
import org.apache.geaflow.dsl.schema.GeaFlowView;
import org.apache.geaflow.dsl.sqlnode.SqlCreateGraph;
import org.apache.geaflow.dsl.sqlnode.SqlCreateTable;
import org.apache.geaflow.dsl.sqlnode.SqlCreateView;
import org.apache.geaflow.utils.client.HttpResponse;
import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

public class ConsoleCatalogTest {

    private final String instanceName = "default";
    private String baseUrl;
    private MockWebServer server;
    private ConsoleCatalog consoleCatalog;
    private GeaFlowGraph graph;
    private GeaFlowGraph graph2;
    private GeaFlowTable table;
    private GeaFlowTable table2;
    private GeaFlowView view;

    @BeforeTest
    public void prepare() throws IOException, SqlParseException {
        GeaFlowDSLParser parser = new GeaFlowDSLParser();
        GQLContext gqlContext = GQLContext.create(new Configuration(), false);

        // setup graph
        String stmtOfGraph = "CREATE GRAPH IF NOT EXISTS modern (\n" + "\tVertex person (\n"
            + "\t  id bigint ID,\n" + "\t  name varchar,\n" + "\t  age int\n" + "\t),\n"
            + "\tVertex software (\n" + "\t  id bigint ID,\n" + "\t  name varchar,\n"
            + "\t  lang varchar\n" + "\t),\n" + "\tEdge knows (\n" + "\t  srcId bigint SOURCE ID,\n"
            + "\t  targetId bigint DESTINATION ID,\n" + "\t  weight double\n" + "\t),\n"
            + "\tEdge created (\n" + "\t  srcId bigint SOURCE ID,\n"
            + "  \ttargetId bigint DESTINATION ID,\n" + "  \tweight double\n" + "\t)\n"
            + ") WITH (\n" + "\tstoreType='memory',\n"
            + "\tgeaflow.dsl.using.vertex.path = 'resource:///data/modern_vertex.txt',\n"
            + "\tgeaflow.dsl.using.edge.path = 'resource:///data/modern_edge.txt'\n" + ")";
        SqlNode sqlNodeOfGraph = parser.parseStatement(stmtOfGraph);
        SqlCreateGraph sqlCreateGraph = (SqlCreateGraph) sqlNodeOfGraph;
        graph = gqlContext.convertToGraph(sqlCreateGraph);

        String stmtOfGraph2 = "CREATE GRAPH IF NOT EXISTS modern2 (\n" + "\tVertex person (\n"
            + "\t  id bigint ID,\n" + "\t  name varchar,\n" + "\t  age int\n" + "\t),\n"
            + "\tVertex software (\n" + "\t  id bigint ID,\n" + "\t  name varchar,\n"
            + "\t  lang varchar\n" + "\t),\n" + "\tEdge knows (\n" + "\t  srcId bigint SOURCE ID,\n"
            + "\t  targetId bigint DESTINATION ID,\n" + "\t  weight double\n" + "\t),\n"
            + "\tEdge created (\n" + "\t  srcId bigint SOURCE ID,\n"
            + "  \ttargetId bigint DESTINATION ID,\n" + "  \tweight double\n" + "\t)\n"
            + ") WITH (\n" + "\tstoreType='memory',\n"
            + "\tgeaflow.dsl.using.vertex.path = 'resource:///data/modern_vertex.txt',\n"
            + "\tgeaflow.dsl.using.edge.path = 'resource:///data/modern_edge.txt'\n" + ")";
        SqlNode sqlNodeOfGraph2 = parser.parseStatement(stmtOfGraph2);
        SqlCreateGraph sqlCreateGraph2 = (SqlCreateGraph) sqlNodeOfGraph2;
        graph2 = gqlContext.convertToGraph(sqlCreateGraph2);
        GraphDescriptor stats = new GraphDescriptor().addEdge(new EdgeDescriptor("0", "created", "person", "software"));
        graph2.setDescriptor(stats);

        // setup table
        String stmtOfTable = "CREATE TABLE IF NOT EXISTS users (\n" + "\tcreateTime bigint,\n"
            + "\tproductId bigint,\n" + "\torderId bigint,\n" + "\tunits bigint,\n"
            + "\tuser_name VARCHAR\n" + ") WITH (\n" + "\ttype='file',\n"
            + "\tgeaflow.dsl.file.path = 'resource:///data/users_correlate2.txt'\n" + ")";
        SqlNode sqlNodeOfTable = parser.parseStatement(stmtOfTable);
        SqlCreateTable sqlCreateTable = (SqlCreateTable) sqlNodeOfTable;
        table = gqlContext.convertToTable(sqlCreateTable);

        String stmtOfTable2 = "CREATE TABLE IF NOT EXISTS users2 (\n" + "\tcreateTime bigint,\n"
            + "\tproductId bigint,\n" + "\torderId bigint,\n" + "\tunits bigint,\n"
            + "\tuser_name VARCHAR\n" + ") WITH (\n" + "\ttype='file',\n"
            + "\tgeaflow.dsl.file.path = 'resource:///data/users_correlate2.txt'\n" + ")";
        SqlNode sqlNodeOfTable2 = parser.parseStatement(stmtOfTable2);
        SqlCreateTable sqlCreateTable2 = (SqlCreateTable) sqlNodeOfTable2;
        table2 = gqlContext.convertToTable(sqlCreateTable2);

        // setup view
        String stmtOfView = "CREATE VIEW IF NOT EXISTS console (count_id, sum_id, max_id, min_id, avg_id, distinct_id, user_name) AS\n"
            + "SELECT\n" + "  1 AS count_id,\n" + "  2 AS sum_id,\n" + "  3 AS max_id,\n"
            + "  4 AS min_id,\n" + "  5 AS avg_id,\n" + "  6 AS distinct_id,\n"
            + "  'test_name' AS user_name";
        SqlNode sqlNodeOfView = parser.parseStatement(stmtOfView);
        SqlCreateView sqlCreateView = (SqlCreateView) sqlNodeOfView;
        view = gqlContext.convertToView(sqlCreateView);

        // setup server
        server = new MockWebServer();
        Dispatcher dispatcher = new Dispatcher() {
            @Override
            public MockResponse dispatch(RecordedRequest recordedRequest)
                throws InterruptedException {
                String path = recordedRequest.getPath();
                Gson gson = new Gson();
                HttpResponse response = new HttpResponse();
                response.setSuccess(true);
                response.setCode("200");
                switch (path) {
                    case "/api/instances/default/graphs/modern":
                        response.setData(gson.toJsonTree(CatalogUtil.convertToGraphModel(graph)));
                        return new MockResponse().setResponseCode(200)
                            .setBody(gson.toJson(response));
                    case "/api/instances/default/graphs/modern2":
                    case "/api/instances/default/tables/users2":
                    case "/api/instances/default/graphs/modern2/endpoints":
                        return new MockResponse().setResponseCode(200).setBody("{success:true}");
                    case "/api/instances/default/graphs":
                        PageList graphList = new PageList();
                        graphList.setList(
                            Collections.singletonList(CatalogUtil.convertToGraphModel(graph)));
                        response.setData(gson.toJsonTree(graphList));
                        return new MockResponse().setResponseCode(200)
                            .setBody(gson.toJson(response));
                    case "/api/instances/default/tables/users":
                        response.setData(gson.toJsonTree(CatalogUtil.convertToTableModel(table)));
                        return new MockResponse().setResponseCode(200)
                            .setBody(gson.toJson(response));
                    case "/api/instances/default/tables":
                        PageList tableList = new PageList();
                        tableList.setList(
                            Collections.singletonList(CatalogUtil.convertToTableModel(table)));
                        response.setData(gson.toJsonTree(tableList));
                        return new MockResponse().setResponseCode(200)
                            .setBody(gson.toJson(response));
                    case "/api/instances":
                        PageList instanceList = new PageList();
                        InstanceModel instanceModel = new InstanceModel();
                        instanceModel.setName(instanceName);
                        instanceModel.setId("13");
                        instanceModel.setComment("test comment");
                        instanceModel.setCreateTime("2023-05-19");
                        instanceModel.setCreatorId("128745");
                        instanceModel.setModifierId("128745");
                        instanceModel.setModifierName("user1");
                        instanceModel.setCreatorName("user1");
                        instanceModel.setModifyTime("2023-05-19");
                        instanceList.setList(Collections.singletonList(instanceModel));
                        response.setData(gson.toJsonTree(instanceList));
                        return new MockResponse().setResponseCode(200)
                            .setBody(gson.toJson(response));
                }
                return null;
            }
        };
        server.setDispatcher(dispatcher);
        server.start();
        baseUrl = "http://" + server.getHostName() + ":" + server.getPort();

        // setup catalog
        consoleCatalog = new ConsoleCatalog();
        Configuration configuration = new Configuration();
        configuration.put(DSLConfigKeys.GEAFLOW_DSL_CATALOG_TOKEN_KEY.getKey(), "test");
        configuration.put(ExecutionConfigKeys.GEAFLOW_GW_ENDPOINT.getKey(), baseUrl);
        consoleCatalog.init(configuration);
    }

    @AfterTest
    public void after() throws IOException {
        server.shutdown();
    }

    @Test
    public void testGraph() {
        consoleCatalog.createGraph(instanceName, graph);
        consoleCatalog.createGraph(instanceName, graph2);
        GeaFlowGraph catalogGraph = (GeaFlowGraph) consoleCatalog.getGraph(instanceName,
            this.graph.getName());
        Assert.assertEquals(catalogGraph.getVertexTables().size(), 2);
        Assert.assertEquals(catalogGraph.getEdgeTables().size(), 2);
        consoleCatalog.describeGraph(instanceName, this.graph.getName());
        consoleCatalog.dropGraph(instanceName, this.graph.getName());
        Set<String> graphsAndTables = consoleCatalog.listGraphAndTable(instanceName);
        Assert.assertTrue(graphsAndTables.contains(graph.getName()));
        Assert.assertTrue(consoleCatalog.isInstanceExists(instanceName));

        CompileCatalog compileCatalog = new CompileCatalog(consoleCatalog);
        compileCatalog.createGraph(instanceName, graph);
        GeaFlowGraph compileGraph = (GeaFlowGraph) compileCatalog.getGraph(instanceName,
            this.graph.getName());
        Assert.assertEquals(compileGraph.getVertexTables().size(), 2);
        Assert.assertEquals(compileGraph.getEdgeTables().size(), 2);
        compileCatalog.describeGraph(instanceName, this.graph.getName());
        compileCatalog.dropGraph(instanceName, this.graph.getName());
        Set<String> compileGraphsAndTables = compileCatalog.listGraphAndTable(instanceName);
        Assert.assertTrue(compileGraphsAndTables.contains(graph.getName()));
        Assert.assertTrue(compileCatalog.isInstanceExists(instanceName));
    }

    @Test
    public void testTableAndView() {
        consoleCatalog.createTable(instanceName, table);
        consoleCatalog.createTable(instanceName, table2);
        consoleCatalog.createView(instanceName, view);
        GeaFlowTable catalogTable = (GeaFlowTable) consoleCatalog.getTable(instanceName,
            this.table.getName());
        Assert.assertEquals(catalogTable.getFields().size(), 5);
        consoleCatalog.describeTable(instanceName, this.table.getName());
        consoleCatalog.dropTable(instanceName, this.table.getName());
        Set<String> graphsAndTables = consoleCatalog.listGraphAndTable(instanceName);
        Assert.assertTrue(graphsAndTables.contains(table.getName()));

        CompileCatalog compileCatalog = new CompileCatalog(consoleCatalog);
        compileCatalog.createTable(instanceName, table);
        compileCatalog.createView(instanceName, view);
        GeaFlowTable compileTable = (GeaFlowTable) compileCatalog.getTable(instanceName,
            this.table.getName());
        Assert.assertEquals(compileTable.getFields().size(), 5);
        compileCatalog.describeTable(instanceName, this.table.getName());
        compileCatalog.dropTable(instanceName, this.table.getName());
        Set<String> compileGraphsAndTables = compileCatalog.listGraphAndTable(instanceName);
        Assert.assertTrue(compileGraphsAndTables.contains(table.getName()));
    }
}
