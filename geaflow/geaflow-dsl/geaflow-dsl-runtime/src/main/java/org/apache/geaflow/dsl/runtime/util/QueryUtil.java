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

package org.apache.geaflow.dsl.runtime.util;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.calcite.schema.Table;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSetOption;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.dsl.common.exception.GeaFlowDSLException;
import org.apache.geaflow.dsl.parser.GeaFlowDSLParser;
import org.apache.geaflow.dsl.planner.GQLContext;
import org.apache.geaflow.dsl.schema.GeaFlowGraph;
import org.apache.geaflow.dsl.schema.GeaFlowTable;
import org.apache.geaflow.dsl.sqlnode.SqlCreateGraph;
import org.apache.geaflow.dsl.sqlnode.SqlCreateTable;
import org.apache.geaflow.dsl.sqlnode.SqlEdgeUsing;
import org.apache.geaflow.dsl.sqlnode.SqlVertexUsing;
import org.apache.geaflow.dsl.util.GQLNodeUtil;
import org.apache.geaflow.dsl.util.StringLiteralUtil;
import org.apache.geaflow.view.graph.GraphViewDesc;
import org.apache.geaflow.view.meta.ViewMetaBookKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QueryUtil {

    private static final Logger LOGGER = LoggerFactory.getLogger(QueryUtil.class);

    public static PreCompileResult preCompile(String script, Configuration config) {
        GeaFlowDSLParser parser = new GeaFlowDSLParser();
        GQLContext gqlContext = GQLContext.create(config, false);
        PreCompileResult preCompileResult = new PreCompileResult();
        try {
            List<SqlNode> sqlNodes = parser.parseMultiStatement(script);
            List<GeaFlowTable> createTablesInScript = new ArrayList<>();
            Map<String, SqlCreateGraph> createGraphs = new HashMap<>();
            for (SqlNode sqlNode : sqlNodes) {
                if (sqlNode instanceof SqlSetOption) {
                    SqlSetOption sqlSetOption = (SqlSetOption) sqlNode;
                    String key = StringLiteralUtil.toJavaString(sqlSetOption.getName());
                    String value = StringLiteralUtil.toJavaString(sqlSetOption.getValue());
                    config.put(key, value);
                } else if (sqlNode instanceof SqlCreateTable) {
                    createTablesInScript.add(gqlContext.convertToTable((SqlCreateTable) sqlNode));
                } else if (sqlNode instanceof SqlCreateGraph) {
                    SqlCreateGraph createGraph = (SqlCreateGraph) sqlNode;
                    SqlIdentifier graphName = gqlContext.completeCatalogObjName(createGraph.getName());
                    if (createGraph.getVertices().getList().stream().anyMatch(node -> node instanceof SqlVertexUsing)
                        || createGraph.getEdges().getList().stream().anyMatch(node -> node instanceof SqlEdgeUsing)) {
                        GeaFlowGraph graph = gqlContext.convertToGraph(createGraph, createTablesInScript);
                        Configuration globalConfig = graph.getConfigWithGlobal(config);
                        if (!QueryUtil.isGraphExists(graph, globalConfig)) {
                            LOGGER.info("insertGraphs: {}", graph.getUniqueName());
                            preCompileResult.addGraph(SchemaUtil.buildGraphViewDesc(graph, globalConfig));
                        }
                    } else {
                        createGraphs.put(graphName.toString(), createGraph);
                    }
                } else if (sqlNode instanceof SqlInsert) {
                    SqlInsert insert = (SqlInsert) sqlNode;
                    SqlIdentifier insertName = gqlContext.completeCatalogObjName(
                        (SqlIdentifier) insert.getTargetTable());
                    SqlIdentifier insertGraphName = GQLNodeUtil.getGraphTableName(insertName);
                    String simpleGraphName = insertName.getComponent(1, 2).getSimple();
                    LOGGER.info("insertGraphName: {}, insertName:{}, simpleGraphName: {}",
                        insertGraphName, insertName, simpleGraphName);
                    if (createGraphs.containsKey(insertGraphName.toString())) {
                        SqlCreateGraph createGraph = createGraphs.get(insertGraphName.toString());
                        GeaFlowGraph graph = gqlContext.convertToGraph(createGraph);
                        LOGGER.info("insertGraphs: {}", graph.getUniqueName());
                        preCompileResult.addGraph(SchemaUtil.buildGraphViewDesc(graph, config));
                    } else {
                        Table graph = gqlContext.getCatalog().getGraph(
                            gqlContext.getCurrentInstance(), simpleGraphName);
                        if (graph != null) {
                            GeaFlowGraph geaFlowGraph = (GeaFlowGraph) graph;
                            geaFlowGraph.getConfig().putAll(gqlContext.keyMapping(geaFlowGraph.getConfig().getConfigMap()));
                            LOGGER.info("insertGraphs: {}", geaFlowGraph.getUniqueName());
                            preCompileResult.addGraph(SchemaUtil.buildGraphViewDesc(geaFlowGraph, config));
                        }
                    }
                }
            }
            return preCompileResult;
        } catch (SqlParseException e) {
            throw new GeaFlowDSLException(e);
        }
    }

    public static boolean isGraphExists(GeaFlowGraph graph, Configuration globalConfig) {
        boolean graphExists;
        try {
            ViewMetaBookKeeper keeper = new ViewMetaBookKeeper(graph.getUniqueName(), globalConfig);
            long lastCheckPointId = keeper.getLatestViewVersion(graph.getUniqueName());
            graphExists = lastCheckPointId >= 0;
        } catch (IOException e) {
            throw new GeaFlowDSLException(e);
        }
        return graphExists;
    }

    public static class PreCompileResult implements Serializable {

        private final List<GraphViewDesc> insertGraphs = new ArrayList<>();

        public void addGraph(GraphViewDesc graphViewDesc) {
            insertGraphs.add(graphViewDesc);
        }

        public List<GraphViewDesc> getInsertGraphs() {
            return insertGraphs;
        }
    }
}
