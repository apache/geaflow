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

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.geaflow.cluster.local.client.LocalEnvironment;
import org.apache.geaflow.common.exception.GeaflowRuntimeException;
import org.apache.geaflow.dsl.common.types.TableField;
import org.apache.geaflow.dsl.schema.GeaFlowGraph;
import org.apache.geaflow.dsl.schema.GeaFlowTable;
import org.apache.geaflow.env.IEnvironment;
import org.apache.geaflow.mcp.server.util.McpLocalFileUtil;
import org.apache.geaflow.mcp.server.util.QueryFormatUtil;
import org.apache.geaflow.mcp.server.util.QueryLocalRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GeaFlowMcpActionsLocalImpl implements GeaFlowMcpActions {

    private static final Logger LOGGER = LoggerFactory.getLogger(GeaFlowMcpActionsLocalImpl.class);

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
            LOGGER.error("Compile error: " + e.getCause().getMessage());
            throw new GeaflowRuntimeException("Compile error: " + e.getCause().getMessage());
        }
        if (graph == null) {
            LOGGER.error("Cannot create graph: " + graphName);
            throw new GeaflowRuntimeException("Cannot create graph: " + graphName);
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
    public String queryGraph(String graphName, String dml) {
        QueryLocalRunner compileRunner = new QueryLocalRunner();
        String ddl = null;
        try {
            ddl = McpLocalFileUtil.readFile(QueryLocalRunner.DSL_STATE_REMOTE_SCHEM_PATH, graphName);
        } catch (Throwable e) {
            LOGGER.error("Cannot get graph schema for: " + graphName);
            throw new GeaflowRuntimeException("Cannot get graph schema for: " + graphName);
        }
        compileRunner.withGraphName(graphName).withGraphDefine(ddl);
        GeaFlowGraph graph;
        try {
            graph = compileRunner.compileGraph();
        } catch (Throwable e) {
            LOGGER.error("Compile error: " + compileRunner.getErrorMsg());
            throw new GeaflowRuntimeException("Compile error: " + compileRunner.getErrorMsg());
        }
        if (graph == null) {
            LOGGER.error("Cannot create graph: " + graphName);
            throw new GeaflowRuntimeException("Cannot create graph: " + graphName);
        }

        QueryLocalRunner runner = new QueryLocalRunner();
        runner.withGraphDefine(ddl);
        runner.withQuery(ddl + "\n" + QueryFormatUtil.makeUseGraph(graphName) + dml);
        try {
            runner.execute();
        } catch (Throwable e) {
            LOGGER.error("Run query error: " + e.getCause().getMessage());
            throw new GeaflowRuntimeException("Run query error: " + e.getCause().getMessage());
        }
        return "run query success: " + dml;
    }

    @Override
    public String queryType(String graphName, String type) {
        QueryLocalRunner compileRunner = new QueryLocalRunner();
        String ddl = null;
        try {
            ddl = McpLocalFileUtil.readFile(QueryLocalRunner.DSL_STATE_REMOTE_SCHEM_PATH, graphName);
        } catch (Throwable e) {
            LOGGER.error("Cannot get graph schema for: " + graphName);
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
            LOGGER.error("Cannot create graph: " + graphName);
            throw new GeaflowRuntimeException("Cannot create graph: " + graphName);
        }

        QueryLocalRunner runner = new QueryLocalRunner();
        runner.withGraphDefine(ddl);
        String dql = null;
        String dirName = "query_result_" + Instant.now().toEpochMilli();
        String resultPath = QueryLocalRunner.DSL_STATE_REMOTE_PATH + "/" + dirName;
        GeaFlowTable resultTable = null;
        for (GeaFlowGraph.VertexTable vertexTable : graph.getVertexTables()) {
            if (vertexTable.getTypeName().equals(type)) {
                dql = QueryFormatUtil.makeResultTable(vertexTable, resultPath)
                        + "\n" + QueryFormatUtil.makeEntityTableQuery(vertexTable);
                resultTable = vertexTable;
            }
        }
        for (GeaFlowGraph.EdgeTable edgeTable : graph.getEdgeTables()) {
            if (edgeTable.getTypeName().equals(type)) {
                dql = QueryFormatUtil.makeResultTable(edgeTable, resultPath)
                        + "\n" + QueryFormatUtil.makeEntityTableQuery(edgeTable);
                resultTable = edgeTable;
            }
        }
        if (resultTable == null) {
            LOGGER.error("Cannot find type: " + type + " in graph: " + graphName);
            throw new GeaflowRuntimeException("Cannot find type: " + type + " in graph: " + graphName);
        }
        runner.withQuery(ddl + "\n" + QueryFormatUtil.makeUseGraph(graphName) + dql);
        String resultContent = "null";
        try {
            runner.execute();
            resultContent = readFile(resultPath);
        } catch (Throwable e) {
            return runner.getErrorMsg();
        }
        String schemaContent = "type: " + type + "\nschema: " + resultTable.getFields().stream()
                .map(TableField::getName).collect(Collectors.joining("|"));
        return schemaContent + "\n" + resultContent;
    }

    @Override
    public String getGraphSchema(String graphName) {
        String ddl = null;
        try {
            ddl = McpLocalFileUtil.readFile(QueryLocalRunner.DSL_STATE_REMOTE_SCHEM_PATH, graphName);
        } catch (Throwable e) {
            LOGGER.error("Cannot get graph schema for: " + graphName);
            return "Cannot get graph schema for: " + graphName;
        }
        return ddl;
    }

    @Override
    public void withUser(String user) {
        this.user = user;
    }

    private String readFile(String path) throws IOException {
        File file = new File(path);
        if (file.isHidden()) {
            return "";
        }
        if (file.isFile()) {
            return IOUtils.toString(new File(path).toURI(), Charset.defaultCharset()).trim();
        }
        File[] files = file.listFiles();
        StringBuilder content = new StringBuilder();
        List<String> readTextList = new ArrayList<>();
        if (files != null) {
            for (File subFile : files) {
                String readText = readFile(subFile.getAbsolutePath());
                if (StringUtils.isBlank(readText)) {
                    continue;
                }
                readTextList.add(readText);
            }
        }
        readTextList = readTextList.stream().sorted().collect(Collectors.toList());
        for (String readText : readTextList) {
            if (content.length() > 0) {
                content.append("\n");
            }
            content.append(readText);
        }
        return content.toString().trim();
    }
}
