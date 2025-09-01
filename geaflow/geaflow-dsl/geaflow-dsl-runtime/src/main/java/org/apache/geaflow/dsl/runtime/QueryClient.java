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

package org.apache.geaflow.dsl.runtime;

import static org.apache.geaflow.common.config.keys.DSLConfigKeys.GEAFLOW_DSL_COMPILE_PHYSICAL_PLAN_ENABLE;

import com.google.common.collect.ImmutableList;
import java.util.HashSet;
import java.util.List;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.util.NlsString;
import org.apache.geaflow.common.config.ConfigHelper;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.dsl.common.compile.CompileContext;
import org.apache.geaflow.dsl.common.compile.CompileResult;
import org.apache.geaflow.dsl.common.compile.FunctionInfo;
import org.apache.geaflow.dsl.common.compile.QueryCompiler;
import org.apache.geaflow.dsl.common.compile.TableInfo;
import org.apache.geaflow.dsl.common.exception.GeaFlowDSLException;
import org.apache.geaflow.dsl.connector.api.TableConnector;
import org.apache.geaflow.dsl.parser.GeaFlowDSLParser;
import org.apache.geaflow.dsl.planner.GQLContext;
import org.apache.geaflow.dsl.runtime.command.IQueryCommand;
import org.apache.geaflow.dsl.runtime.engine.GeaFlowQueryEngine;
import org.apache.geaflow.dsl.runtime.util.AnalyticsResultFormatter;
import org.apache.geaflow.dsl.sqlnode.SqlCreateFunction;
import org.apache.geaflow.dsl.sqlnode.SqlCreateGraph;
import org.apache.geaflow.dsl.sqlnode.SqlCreateTable;
import org.apache.geaflow.dsl.sqlnode.SqlTableProperty;
import org.apache.geaflow.dsl.sqlnode.SqlUseInstance;
import org.apache.geaflow.dsl.util.SqlNodeUtil;
import org.apache.geaflow.plan.PipelinePlanBuilder;
import org.apache.geaflow.plan.graph.PipelineGraph;
import org.apache.geaflow.plan.visualization.JsonPlanGraphVisualization;
import org.apache.geaflow.runtime.pipeline.PipelineContext;
import org.apache.geaflow.runtime.pipeline.PipelineTaskType;
import org.apache.geaflow.runtime.pipeline.task.PipelineTaskContext;

public class QueryClient implements QueryCompiler {

    private final GeaFlowDSLParser parser = new GeaFlowDSLParser();


    public QueryClient() {
    }

    /**
     * Execute multi-query at once.
     *
     * @param sql     The sql script which contains multi-query to execute.
     * @param context The context for query.
     */
    public void executeQuery(String sql, QueryContext context) {
        try {
            List<SqlNode> sqlNodes = parser.parseMultiStatement(sql);
            for (SqlNode sqlNode : sqlNodes) {
                executeQuery(sqlNode, context);
            }
        } catch (SqlParseException e) {
            throw new GeaFlowDSLException("Error in execute query: \n" + sql, e);
        }
    }

    /**
     * Execute single query.
     *
     * @param sql     The sql to execute.
     * @param context The context for executor engine.
     * @return The result for the query.
     */
    public QueryResult executeSingleQuery(String sql, QueryContext context) {

        try {
            SqlNode sqlNode = parser.parseStatement(sql);
            return executeQuery(sqlNode, context);
        } catch (SqlParseException e) {
            throw new GeaFlowDSLException("Error in execute query: \n" + sql, e);
        }
    }


    private QueryResult executeQuery(SqlNode sqlNode, QueryContext context) {
        IQueryCommand command = context.getCommand(sqlNode);
        return command.execute(context);
    }

    @Override
    public CompileResult compile(String script, CompileContext context) {
        PipelineContext pipelineContext = new PipelineContext(PipelineTaskType.CompileTask.name(),
            new Configuration(context.getConfig()));
        PipelineTaskContext pipelineTaskCxt = new PipelineTaskContext(0L, pipelineContext);
        QueryEngine engineContext = new GeaFlowQueryEngine(pipelineTaskCxt);
        QueryContext queryContext = QueryContext.builder()
            .setEngineContext(engineContext)
            .setCompile(true)
            .setTraversalParallelism(-1)
            .build();
        queryContext.putConfigParallelism(context.getParallelisms());
        executeQuery(script, queryContext);

        CompileResult compileResult = new CompileResult();
        // Get current schema before finish.
        compileResult.setCurrentResultType(queryContext.getCurrentResultType());

        queryContext.finish();

        compileResult.setSourceTables(queryContext.getReferSourceTables());
        compileResult.setTargetTables(queryContext.getReferTargetTables());
        compileResult.setSourceGraphs(queryContext.getReferSourceGraphs());
        compileResult.setTargetGraphs(queryContext.getReferTargetGraphs());

        boolean needPlan = ConfigHelper.getBooleanOrDefault(context.getConfig(), GEAFLOW_DSL_COMPILE_PHYSICAL_PLAN_ENABLE.getKey(), Boolean.TRUE);
        if (needPlan) {
            PipelinePlanBuilder pipelinePlanBuilder = new PipelinePlanBuilder();
            PipelineGraph pipelineGraph = pipelinePlanBuilder.buildPlan(pipelineContext);
            pipelinePlanBuilder.optimizePlan(pipelineContext.getConfig());
            JsonPlanGraphVisualization visualization = new JsonPlanGraphVisualization(pipelineGraph);
            compileResult.setPhysicPlan(visualization.getJsonPlan());
        }
        return compileResult;
    }


    @Override
    public String formatOlapResult(String script, Object queryResult, CompileContext context) {
        context.getConfig().put("needPhysicalPlan", "false");
        CompileResult compileResult = compile(script, context);
        return AnalyticsResultFormatter.formatResult(queryResult, compileResult.getCurrentResultType());
    }

    @Override
    public Set<FunctionInfo> getUnResolvedFunctions(String script, CompileContext context) {
        try {
            List<SqlNode> sqlNodes = parser.parseMultiStatement(script);
            GQLContext gqlContext = GQLContext.create(new Configuration(context.getConfig()), true);
            Set<FunctionInfo> functions = new HashSet<>();
            for (SqlNode sqlNode : sqlNodes) {
                if (sqlNode instanceof SqlCreateFunction) {
                    SqlCreateFunction function = (SqlCreateFunction) sqlNode;
                    String functionName = function.getFunctionName().toString();
                    String instanceName = gqlContext.getCurrentInstance();
                    FunctionInfo functionInfo = new FunctionInfo(instanceName, functionName);
                    functions.add(functionInfo);
                } else if (sqlNode instanceof SqlUseInstance) {
                    SqlUseInstance useInstance = (SqlUseInstance) sqlNode;
                    String instanceName = useInstance.getInstance().toString();
                    gqlContext.setCurrentInstance(instanceName);
                } else {
                    List<SqlUnresolvedFunction> unresolvedFunctions = SqlNodeUtil.findUnresolvedFunctions(sqlNode);
                    for (SqlUnresolvedFunction unresolvedFunction : unresolvedFunctions) {
                        String name = unresolvedFunction.getName();
                        SqlFunction sqlFunction = gqlContext.findSqlFunction(gqlContext.getCurrentInstance(), name);
                        if (sqlFunction == null) {
                            FunctionInfo functionInfo = new FunctionInfo(gqlContext.getCurrentInstance(), name);
                            functions.add(functionInfo);
                        }
                    }
                }
            }
            return functions;
        } catch (Exception e) {
            throw new GeaFlowDSLException("Error in parser dsl", e);
        }
    }

    @Override
    public Set<String> getEnginePlugins() {
        Set<String> typs = new HashSet<>();
        ServiceLoader<TableConnector> connectors = ServiceLoader.load(TableConnector.class);
        for (TableConnector connector : connectors) {
            typs.add(connector.getType().toUpperCase());
        }
        return typs;
    }

    @Override
    public Set<String> getDeclaredTablePlugins(String script, CompileContext context) {
        try {
            List<SqlNode> sqlNodes = parser.parseMultiStatement(script);
            Set<String> plugins = new HashSet<>();

            for (SqlNode sqlNode : sqlNodes) {
                if (sqlNode instanceof SqlCreateTable) {
                    SqlNodeList properties = ((SqlCreateTable) sqlNode).getProperties();
                    for (SqlNode property : properties) {
                        if (property instanceof SqlTableProperty) {
                            ImmutableList<String> names = ((SqlTableProperty) property).getKey().names;
                            String key = names.get(0);
                            if (key.equals("type")) {
                                NlsString nlsString =
                                    (NlsString) ((SqlCharStringLiteral) ((SqlTableProperty) property).getValue()).getValue();
                                plugins.add(nlsString.getValue());
                            }
                        }
                    }
                }
            }
            return plugins;
        } catch (Exception e) {
            throw new GeaFlowDSLException("Error in parser dsl", e);
        }
    }

    @Override
    public Set<TableInfo> getUnResolvedTables(String script, CompileContext context) {
        try {
            List<SqlNode> sqlNodes = parser.parseMultiStatement(script);
            GQLContext gqlContext = GQLContext.create(new Configuration(context.getConfig()), true);

            Set<TableInfo> declaredTables = new HashSet<>();
            Set<TableInfo> unResolvedTables = new HashSet<>();
            for (SqlNode sqlNode : sqlNodes) {
                if (sqlNode instanceof SqlCreateTable) {
                    ImmutableList<String> names = ((SqlCreateTable) sqlNode).getName().names;
                    String tableName = names.get(0);
                    String instanceName = gqlContext.getCurrentInstance();
                    declaredTables.add(new TableInfo(instanceName, tableName));

                } else if (sqlNode instanceof SqlCreateGraph) {
                    ImmutableList<String> names = ((SqlCreateGraph) sqlNode).getName().names;
                    String tableName = names.get(0);
                    String instanceName = gqlContext.getCurrentInstance();
                    declaredTables.add(new TableInfo(instanceName, tableName));

                } else if (sqlNode instanceof SqlUseInstance) {
                    SqlUseInstance useInstance = (SqlUseInstance) sqlNode;
                    String instanceName = useInstance.getInstance().toString();
                    gqlContext.setCurrentInstance(instanceName);

                } else {
                    Set<String> usedTables = SqlNodeUtil.findUsedTables(sqlNode);
                    String instanceName = gqlContext.getCurrentInstance();
                    for (String usedTable : usedTables) {
                        unResolvedTables.add(new TableInfo(instanceName, usedTable));
                    }
                }
            }
            return unResolvedTables.stream().filter(e -> !declaredTables.contains(e)).collect(Collectors.toSet());
        } catch (Exception e) {
            throw new GeaFlowDSLException("Error in parser dsl", e);
        }
    }
}
