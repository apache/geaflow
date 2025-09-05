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

package org.apache.geaflow.mcp.server.util;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import org.apache.geaflow.cluster.system.ClusterMetaStore;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.common.config.keys.DSLConfigKeys;
import org.apache.geaflow.common.config.keys.FrameworkConfigKeys;
import org.apache.geaflow.dsl.common.compile.CompileContext;
import org.apache.geaflow.dsl.runtime.QueryClient;
import org.apache.geaflow.dsl.runtime.QueryContext;
import org.apache.geaflow.dsl.runtime.QueryEngine;
import org.apache.geaflow.dsl.runtime.engine.GQLPipeLine;
import org.apache.geaflow.dsl.runtime.engine.GeaFlowQueryEngine;
import org.apache.geaflow.dsl.schema.GeaFlowGraph;
import org.apache.geaflow.env.Environment;
import org.apache.geaflow.env.EnvironmentFactory;
import org.apache.geaflow.file.FileConfigKeys;
import org.apache.geaflow.runtime.pipeline.PipelineContext;
import org.apache.geaflow.runtime.pipeline.PipelineTaskType;
import org.apache.geaflow.runtime.pipeline.task.PipelineTaskContext;

public class QueryLocalRunner {


    public static final String DSL_STATE_REMOTE_PATH = "/tmp/dsl/mcp";
    public static final String DSL_STATE_REMOTE_SCHEM_PATH = "/tmp/dsl/mcp/schema";
    private String graphDefine;
    private String graphName;
    private String errorMsg;
    private String query;
    private final Map<String, String> config = new HashMap<>();

    public QueryLocalRunner withConfig(Map<String, String> config) {
        this.config.putAll(config);
        return this;
    }

    public QueryLocalRunner withConfig(String key, Object value) {
        this.config.put(key, String.valueOf(value));
        return this;
    }

    public QueryLocalRunner withGraphDefine(String graphDefine) {
        this.graphDefine = Objects.requireNonNull(graphDefine);
        return this;
    }

    public QueryLocalRunner withGraphName(String graphName) {
        this.graphName = Objects.requireNonNull(graphName);
        return this;
    }

    public QueryLocalRunner withQuery(String query) {
        this.query = Objects.requireNonNull(query);
        return this;
    }

    public String getErrorMsg() {
        return errorMsg;
    }

    public GeaFlowGraph compileGraph() throws Exception {
        Map<String, String> config = new HashMap<>();
        config.put(DSLConfigKeys.GEAFLOW_DSL_WINDOW_SIZE.getKey(), String.valueOf(-1L));
        if (this.graphDefine == null) {
            throw new RuntimeException("Create graph ddl is empty");
        }
        config.put(FrameworkConfigKeys.SYSTEM_STATE_BACKEND_TYPE.getKey(), "memory");
        config.put(FileConfigKeys.ROOT.getKey(), DSL_STATE_REMOTE_PATH);
        String fileName = McpLocalFileUtil.createAndWriteFile(DSL_STATE_REMOTE_PATH, this.graphDefine);
        config.put(DSLConfigKeys.GEAFLOW_DSL_QUERY_PATH.getKey(), DSL_STATE_REMOTE_PATH + "/" + fileName);
        config.put(DSLConfigKeys.GEAFLOW_DSL_QUERY_PATH_TYPE.getKey(), "file");
        config.putAll(this.config);
        Environment environment = EnvironmentFactory.onLocalEnvironment();
        environment.getEnvironmentContext().withConfig(config);
        // Compile graph name
        CompileContext compileContext = new CompileContext();
        config.put(DSLConfigKeys.GEAFLOW_DSL_COMPILE_PHYSICAL_PLAN_ENABLE.getKey(), "false");
        compileContext.setConfig(config);
        PipelineContext pipelineContext = new PipelineContext(PipelineTaskType.CompileTask.name(),
                new Configuration(compileContext.getConfig()));
        PipelineTaskContext pipelineTaskCxt = new PipelineTaskContext(0L, pipelineContext);
        QueryEngine engineContext = new GeaFlowQueryEngine(pipelineTaskCxt);
        QueryContext queryContext = QueryContext.builder()
                .setEngineContext(engineContext)
                .setCompile(true)
                .setTraversalParallelism(-1)
                .build();
        QueryClient queryClient = new QueryClient();
        queryClient.executeQuery(this.graphDefine, queryContext);
        return queryContext.getGraph(graphName);
    }

    public QueryLocalRunner execute() throws Exception {
        Map<String, String> config = new HashMap<>();
        config.put(DSLConfigKeys.GEAFLOW_DSL_WINDOW_SIZE.getKey(), String.valueOf(-1L));
        if (this.graphDefine == null) {
            throw new RuntimeException("Create graph ddl is empty");
        }
        config.put(FrameworkConfigKeys.SYSTEM_STATE_BACKEND_TYPE.getKey(), "memory");
        config.put(FileConfigKeys.ROOT.getKey(), DSL_STATE_REMOTE_PATH);
        String fileName = McpLocalFileUtil.createAndWriteFile(DSL_STATE_REMOTE_PATH, this.query);
        config.put(DSLConfigKeys.GEAFLOW_DSL_QUERY_PATH.getKey(), DSL_STATE_REMOTE_PATH + "/" + fileName);
        config.put(DSLConfigKeys.GEAFLOW_DSL_QUERY_PATH_TYPE.getKey(), "file");
        config.put(FrameworkConfigKeys.BATCH_NUMBER_PER_CHECKPOINT.getKey(), "1");
        config.putAll(this.config);

        Environment environment = EnvironmentFactory.onLocalEnvironment();
        environment.getEnvironmentContext().withConfig(config);

        GQLPipeLine gqlPipeLine = new GQLPipeLine(environment, 0);

        try {
            gqlPipeLine.execute();
        } finally {
            environment.shutdown();
            ClusterMetaStore.close();
        }
        return this;
    }
}
