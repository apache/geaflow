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

package org.apache.geaflow.mcp.server;

import org.apache.geaflow.cluster.local.client.LocalEnvironment;
import org.apache.geaflow.dsl.schema.GeaFlowGraph;
import org.apache.geaflow.env.IEnvironment;
import org.apache.geaflow.mcp.server.util.McpLocalFileUtil;
import org.apache.geaflow.mcp.server.util.QueryLocalRunner;

import java.util.Map;

/**
 * Alipay.com Inc
 * Copyright (c) 2004-2025 All Rights Reserved.
 *
 * @author lt on 2025/9/1.
 */
public class GeaFlowMcpActionsLocalImpl implements GeaFlowMcpActions {

    private Map<String, Object> configs;
    private String user;
    private IEnvironment localEnv = new LocalEnvironment();

    public GeaFlowMcpActionsLocalImpl(Map<String, Object> configs) {
        this.configs = configs;
    }

    @Override
    public String createGraph(String graphName, String ddl) {
        QueryLocalRunner runner = new QueryLocalRunner();
        runner.withGraphName(graphName).withGraphDefine(ddl);
        GeaFlowGraph graph;
        try {
            graph = runner.compileGraph();
        } catch (Throwable e) {
            return runner.getErrorMsg();
        }
        if (graph == null) {
            throw new RuntimeException("Cannot create graph: " + graphName);
        }
        //Store graph ddl to schema
        try {
            McpLocalFileUtil.createAndWriteFile(
                    QueryLocalRunner.DSL_STATE_REMOTE_SCHEM_PATH, ddl, graphName);
        } catch (Throwable e) {
            return runner.getErrorMsg();
        }
        return "Create graph " + graphName + " success.";
    }

    @Override
    public String queryGraph(String graphName, String type) {
        QueryLocalRunner compileRunner = new QueryLocalRunner();
        String ddl = null;
        try {
            ddl = McpLocalFileUtil.readFile(QueryLocalRunner.DSL_STATE_REMOTE_SCHEM_PATH, graphName);
        } catch (Throwable e) {
            return "Cannot get graph schema for: " + graphName;
        }
        compileRunner.withGraphName(graphName).withGraphDefine(ddl);
        GeaFlowGraph graph;
        try {
            graph = compileRunner.compileGraph();
        } catch (Throwable e) {
            return compileRunner.getErrorMsg();
        }
        if (graph == null) {
            throw new RuntimeException("Cannot create graph: " + graphName);
        }

        QueryLocalRunner runner = new QueryLocalRunner();
        runner.withGraphDefine(ddl);
        String usingGraph = "USE GRAPH " + graphName + ";\n";
        String dql = null;
        for (GeaFlowGraph.VertexTable vertexTable : graph.getVertexTables()) {
            if (vertexTable.getTypeName().equals(type)) {
                dql = "Match(a:" + type + ")";
            }
        }
        for (GeaFlowGraph.EdgeTable edgeTable : graph.getEdgeTables()) {
            if (edgeTable.getTypeName().equals(type)) {
                dql = "Match()-[e:" + type + "]-()";
            }
        }
        runner.withQuery(ddl + "\n" + usingGraph + dql);
        try {
            runner.execute();
        } catch (Throwable e) {
            return runner.getErrorMsg();
        }
        return "run query success: " + dql;
    }

    @Override
    public void withUser(String user) {
        this.user = user;
    }
}
