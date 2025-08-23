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

package org.apache.geaflow.dsl.runtime.engine;

import static org.apache.geaflow.common.config.keys.FrameworkConfigKeys.BATCH_NUMBER_PER_CHECKPOINT;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.common.config.keys.DSLConfigKeys;
import org.apache.geaflow.common.exception.GeaflowRuntimeException;
import org.apache.geaflow.common.utils.CheckpointUtil;
import org.apache.geaflow.dsl.connector.file.FileConstants;
import org.apache.geaflow.dsl.runtime.QueryClient;
import org.apache.geaflow.dsl.runtime.QueryContext;
import org.apache.geaflow.dsl.runtime.QueryEngine;
import org.apache.geaflow.dsl.runtime.util.QueryUtil;
import org.apache.geaflow.dsl.runtime.util.QueryUtil.PreCompileResult;
import org.apache.geaflow.env.Environment;
import org.apache.geaflow.pipeline.IPipelineResult;
import org.apache.geaflow.pipeline.Pipeline;
import org.apache.geaflow.pipeline.PipelineFactory;
import org.apache.geaflow.pipeline.callback.ICallbackFunction;
import org.apache.geaflow.pipeline.callback.TaskCallBack;
import org.apache.geaflow.pipeline.task.IPipelineTaskContext;
import org.apache.geaflow.pipeline.task.PipelineTask;
import org.apache.geaflow.view.IViewDesc.BackendType;
import org.apache.geaflow.view.graph.GraphViewDesc;
import org.apache.geaflow.view.meta.ViewMetaBookKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GQLPipeLine {

    private static final Logger LOGGER = LoggerFactory.getLogger(GQLPipeLine.class);

    private static final String GQL_FILE_NAME = "user.gql";

    private final Environment environment;

    private GQLPipelineHook pipelineHook;

    private final int timeWaitSeconds;

    private final Map<String, Integer> parallelismConfigMap;

    public GQLPipeLine(Environment environment, Map<String, Integer> parallelismConfigMap) {
        this(environment, -1, parallelismConfigMap);
    }

    public GQLPipeLine(Environment environment, int timeOutSeconds) {
        this(environment, timeOutSeconds, null);
    }

    public GQLPipeLine(Environment environment, int timeWaitSeconds,
                       Map<String, Integer> parallelismConfigMap) {
        this.environment = environment;
        this.timeWaitSeconds = timeWaitSeconds;
        this.parallelismConfigMap = parallelismConfigMap;
    }

    public void setPipelineHook(GQLPipelineHook pipelineHook) {
        this.pipelineHook = pipelineHook;
    }

    public void execute() throws Exception {
        Configuration configuration = environment.getEnvironmentContext().getConfig();
        String queryPath = configuration.getString(DSLConfigKeys.GEAFLOW_DSL_QUERY_PATH, GQL_FILE_NAME);
        String script;
        if (queryPath.startsWith(FileConstants.PREFIX_JAVA_RESOURCE)) {
            script = IOUtils.resourceToString(
                queryPath.substring(FileConstants.PREFIX_JAVA_RESOURCE.length()),
                Charset.defaultCharset());
        } else {
            String pathType = configuration.getString(DSLConfigKeys.GEAFLOW_DSL_QUERY_PATH_TYPE, FileConstants.PREFIX_JAVA_RESOURCE);
            if (pathType.equals(FileConstants.PREFIX_JAVA_RESOURCE)) {
                script = IOUtils.resourceToString(queryPath, Charset.defaultCharset(),
                    GQLPipeLine.class.getClassLoader());
            } else {
                script = FileUtils.readFileToString(new File(queryPath), Charset.defaultCharset());
            }
        }
        LOGGER.info("queryPath:{}", queryPath);

        if (pipelineHook != null) {
            script = pipelineHook.rewriteScript(script, configuration);
        }
        LOGGER.info("execute query:\n{}", script);
        if (script == null) {
            throw new IllegalArgumentException("Cannot get script from certain query path.");
        }
        Pipeline pipeline = PipelineFactory.buildPipeline(environment);
        LOGGER.info("Submit pipeline task ...");
        PreCompileResult compileResult = QueryUtil.preCompile(script, configuration);
        TaskCallBack callBack = pipeline.submit(new GQLPipelineTask(script, pipelineHook,
            parallelismConfigMap));
        callBack.addCallBack(new SaveGraphWriteVersionCallbackFunction(configuration, compileResult));
        LOGGER.info("Execute pipeline task");
        IPipelineResult result = pipeline.execute();
        LOGGER.info("Submit finished, waiting future result ...");
        if (timeWaitSeconds > 0) {
            CompletableFuture future = CompletableFuture.supplyAsync(() -> result.get());
            future.get(timeWaitSeconds, TimeUnit.SECONDS);
        } else if (timeWaitSeconds == 0) {
            result.get();
        }
    }

    private static class SaveGraphWriteVersionCallbackFunction implements ICallbackFunction {

        private static final Logger LOGGER = LoggerFactory.getLogger(SaveGraphWriteVersionCallbackFunction.class);

        private final Configuration conf;
        private final List<GraphViewDesc> insertGraphs;
        private final long checkpointDuration;

        public SaveGraphWriteVersionCallbackFunction(Configuration conf, PreCompileResult compileResult) {
            this.conf = conf;
            this.checkpointDuration = conf.getLong(BATCH_NUMBER_PER_CHECKPOINT);
            this.insertGraphs = compileResult.getInsertGraphs();
        }

        @Override
        public void window(long windowId) {
            if (CheckpointUtil.needDoCheckpoint(windowId, checkpointDuration)) {
                for (GraphViewDesc graphViewDesc : insertGraphs) {
                    if (graphViewDesc.getBackend().equals(BackendType.Memory)) {
                        continue;
                    }
                    long checkpointId = graphViewDesc.getCheckpoint(windowId);
                    try {
                        ViewMetaBookKeeper keeper = new ViewMetaBookKeeper(graphViewDesc.getName(), conf);
                        keeper.saveViewVersion(checkpointId);
                        keeper.archive();
                        LOGGER.info("save latest version for graph: {}, version id: {}", keeper.getViewName(),
                            checkpointId);
                    } catch (IOException e) {
                        throw new GeaflowRuntimeException("fail to do save latest version for: "
                            + graphViewDesc.getName() + ", windowId is: " + windowId + ", checkpointId is: "
                            + checkpointId, e);
                    }
                }
            }
        }

        @Override
        public void terminal() {

        }
    }

    public static class GQLPipelineTask implements PipelineTask {

        private final String script;

        private final GQLPipelineHook pipelineHook;

        private final Map<String, Integer> parallelismConfigMap;

        public GQLPipelineTask(String script, GQLPipelineHook pipelineHook,
                               Map<String, Integer> parallelismConfigMap) {
            this.script = script;
            this.pipelineHook = pipelineHook;
            this.parallelismConfigMap = parallelismConfigMap;
        }

        @Override
        public void execute(IPipelineTaskContext pipelineTaskCxt) {
            QueryClient queryClient = new QueryClient();
            QueryEngine engineContext = new GeaFlowQueryEngine(pipelineTaskCxt);
            QueryContext queryContext = QueryContext.builder()
                .setEngineContext(engineContext)
                .setCompile(false)
                .build();
            if (pipelineHook != null) {
                pipelineHook.beforeExecute(queryClient, queryContext);
            }
            if (parallelismConfigMap != null) {
                queryContext.putConfigParallelism(parallelismConfigMap);
            }
            queryClient.executeQuery(script, queryContext);
            if (pipelineHook != null) {
                pipelineHook.afterExecute(queryClient, queryContext);
            }
            queryContext.finish();
        }
    }

    public interface GQLPipelineHook {

        String rewriteScript(String script, Configuration configuration);

        void beforeExecute(QueryClient queryClient, QueryContext queryContext);

        void afterExecute(QueryClient queryClient, QueryContext queryContext);
    }
}
