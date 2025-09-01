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

package org.apache.geaflow.analytics.service.server;

import static org.apache.geaflow.analytics.service.config.AnalyticsServiceConfigKeys.ANALYTICS_COMPILE_SCHEMA_ENABLE;
import static org.apache.geaflow.analytics.service.config.AnalyticsServiceConfigKeys.ANALYTICS_QUERY;
import static org.apache.geaflow.analytics.service.config.AnalyticsServiceConfigKeys.ANALYTICS_SERVICE_REGISTER_ENABLE;
import static org.apache.geaflow.common.config.keys.DSLConfigKeys.GEAFLOW_DSL_COMPILE_PHYSICAL_PLAN_ENABLE;

import com.google.common.base.Preconditions;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.geaflow.analytics.service.config.AnalyticsServiceConfigKeys;
import org.apache.geaflow.analytics.service.query.QueryError;
import org.apache.geaflow.analytics.service.query.QueryInfo;
import org.apache.geaflow.analytics.service.query.QueryResults;
import org.apache.geaflow.cluster.exception.ComponentUncaughtExceptionHandler;
import org.apache.geaflow.cluster.response.ResponseResult;
import org.apache.geaflow.common.blocking.map.BlockingMap;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.common.rpc.HostAndPort;
import org.apache.geaflow.common.utils.ProcessUtil;
import org.apache.geaflow.common.utils.ThreadUtil;
import org.apache.geaflow.dsl.common.compile.CompileContext;
import org.apache.geaflow.dsl.common.compile.CompileResult;
import org.apache.geaflow.dsl.runtime.QueryClient;
import org.apache.geaflow.metaserver.internal.MetaServerClient;
import org.apache.geaflow.metaserver.service.NamespaceType;
import org.apache.geaflow.pipeline.service.IPipelineServiceExecutorContext;
import org.apache.geaflow.pipeline.service.IServiceServer;
import org.apache.geaflow.pipeline.service.PipelineService;
import org.apache.geaflow.plan.PipelinePlanBuilder;
import org.apache.geaflow.plan.graph.PipelineGraph;
import org.apache.geaflow.runtime.core.scheduler.result.ExecutionResult;
import org.apache.geaflow.runtime.core.scheduler.result.IExecutionResult;
import org.apache.geaflow.runtime.pipeline.PipelineContext;
import org.apache.geaflow.runtime.pipeline.service.PipelineServiceContext;
import org.apache.geaflow.runtime.pipeline.service.PipelineServiceExecutorContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractAnalyticsServiceServer implements IServiceServer {

    private static final Logger LOGGER = LoggerFactory.getLogger(
        AbstractAnalyticsServiceServer.class);

    private static final String ANALYTICS_SERVICE_PREFIX = "analytics-service-";
    private static final String SERVER_EXECUTOR = "server-executor";

    protected int port;
    protected int maxRequests;
    protected boolean running;
    protected PipelineService pipelineService;
    protected PipelineServiceExecutorContext serviceExecutorContext;
    protected BlockingQueue<QueryInfo> requestBlockingQueue;
    protected BlockingMap<String, Future<IExecutionResult>> responseBlockingMap;
    protected BlockingQueue<Long> cancelRequestBlockingQueue;
    protected BlockingQueue<Object> cancelResponseBlockingQueue;
    protected MetaServerClient metaServerClient;
    protected Semaphore semaphore;
    private ExecutorService executorService;

    protected Configuration configuration;
    protected boolean enableCompileSchema;

    @Override
    public void init(IPipelineServiceExecutorContext context) {
        this.serviceExecutorContext = (PipelineServiceExecutorContext) context;
        this.pipelineService = this.serviceExecutorContext.getPipelineService();
        this.configuration = context.getConfiguration();
        this.port = configuration.getInteger(AnalyticsServiceConfigKeys.ANALYTICS_SERVICE_PORT);
        this.maxRequests = configuration.getInteger(
            AnalyticsServiceConfigKeys.MAX_REQUEST_PER_SERVER);
        this.requestBlockingQueue = new LinkedBlockingQueue<>(maxRequests);
        this.responseBlockingMap = new BlockingMap<>();
        this.cancelRequestBlockingQueue = new LinkedBlockingQueue<>(maxRequests);
        this.cancelResponseBlockingQueue = new LinkedBlockingQueue<>(maxRequests);
        this.semaphore = new Semaphore(maxRequests);
        this.executorService = Executors.newFixedThreadPool(this.maxRequests,
            ThreadUtil.namedThreadFactory(true, SERVER_EXECUTOR,
                new ComponentUncaughtExceptionHandler()));
        this.enableCompileSchema = configuration.getBoolean(ANALYTICS_COMPILE_SCHEMA_ENABLE);
    }

    @Override
    public void stopServer() {
        this.running = false;
        if (this.metaServerClient != null) {
            this.metaServerClient.close();
        }
    }

    public static QueryResults getQueryResults(QueryInfo queryInfo,
                                               BlockingMap<String, Future<IExecutionResult>> responseBlockingMap) throws Exception {
        Future<IExecutionResult> resultFuture = responseBlockingMap.get(queryInfo.getQueryId());
        IExecutionResult result = resultFuture.get();
        QueryResults queryResults;
        String queryId = queryInfo.getQueryId();
        if (result.isSuccess()) {
            List<List<ResponseResult>> responseResult = (List<List<ResponseResult>>) result.getResult();
            queryResults = new QueryResults(queryId, responseResult);
        } else {
            String errorMsg = result.getError().toString();
            queryResults = new QueryResults(queryId, new QueryError(errorMsg));
        }
        queryResults.setResultMeta(queryInfo.getScriptSchema());
        return queryResults;
    }

    protected void waitForExecuted() {
        registerServiceInfo();

        while (this.running) {
            try {
                QueryInfo queryInfo = requestBlockingQueue.take();
                final String queryScript = queryInfo.getQueryScript();
                final String queryId = queryInfo.getQueryId();

                if (enableCompileSchema) {
                    try {
                        CompileResult compileResult = compileQuerySchema(queryScript, configuration);
                        RelDataType relDataType = compileResult.getCurrentResultType();
                        queryInfo.setScriptSchema(relDataType);
                    } catch (Throwable e) {
                        // Set error code if precompile failed.
                        LOGGER.error("precompile query: {} failed", queryInfo, e);
                        Future<IExecutionResult> future = executorService.submit(
                            () -> new ExecutionResult(queryId, new QueryError(ExceptionUtils.getStackTrace(e)), false));
                        responseBlockingMap.put(queryId, future);
                        continue;
                    }
                }

                Future<IExecutionResult> future = executorService.submit(() -> {
                    try {
                        return executeQuery(queryScript);
                    } catch (Throwable e) {
                        LOGGER.error("execute query: {} failed", queryInfo, e);
                        return new ExecutionResult(queryId, new QueryError(ExceptionUtils.getStackTrace(e)), false);
                    }
                });

                responseBlockingMap.put(queryId, future);
            } catch (Throwable t) {
                if (this.running) {
                    LOGGER.error("analytics service abnormal {}", t.getMessage(), t);
                }
            }
        }
    }

    protected static CompileResult compileQuerySchema(String query, Configuration configuration) {
        QueryClient queryManager = new QueryClient();
        CompileContext compileContext = new CompileContext();
        compileContext.setConfig(configuration.getConfigMap());
        compileContext.getConfig().put(GEAFLOW_DSL_COMPILE_PHYSICAL_PLAN_ENABLE.getKey(),
            Boolean.FALSE.toString());
        return queryManager.compile(query, compileContext);
    }

    private void registerServiceInfo() {
        // First initialize analytics service instance and only in service 0.
        if (serviceExecutorContext.getDriverIndex() == 0) {
            String analyticsQuery = serviceExecutorContext.getPipelineContext().getConfig()
                .getString(ANALYTICS_QUERY);
            Preconditions.checkArgument(analyticsQuery != null, "analytics query must be not null");
            executeQuery(analyticsQuery);
            LOGGER.info("service index {} analytics query execute successfully",
                serviceExecutorContext.getDriverIndex());
        }
        // Register analytics service info.
        if (serviceExecutorContext.getConfiguration()
            .getBoolean(ANALYTICS_SERVICE_REGISTER_ENABLE)) {
            metaServerClient = new MetaServerClient(serviceExecutorContext.getConfiguration());
            metaServerClient.registerService(NamespaceType.DEFAULT,
                ANALYTICS_SERVICE_PREFIX + serviceExecutorContext.getDriverIndex(),
                new HostAndPort(ProcessUtil.getHostIp(), port));
            LOGGER.info("service index {} register analytics service {}:{}",
                serviceExecutorContext.getDriverIndex(), ProcessUtil.getHostIp(), port);
        }
        this.running = true;
    }

    private IExecutionResult executeQuery(String query) {
        // User pipeline Task.
        PipelineContext pipelineContext = new PipelineContext(
            serviceExecutorContext.getPipelineContext().getName(),
            serviceExecutorContext.getPipelineContext().getConfig());
        serviceExecutorContext.getPipelineContext().getViewDescMap().forEach(
            (s, iViewDesc) -> pipelineContext.addView(iViewDesc));
        PipelineServiceContext serviceContext = new PipelineServiceContext(
            System.currentTimeMillis(), pipelineContext, query);
        pipelineService.execute(serviceContext);
        PipelinePlanBuilder pipelinePlanBuilder = new PipelinePlanBuilder();
        // 1. Build pipeline graph plan.
        PipelineGraph pipelineGraph = pipelinePlanBuilder.buildPlan(pipelineContext);
        // 2. Opt pipeline graph plan.
        pipelinePlanBuilder.optimizePlan(pipelineContext.getConfig());
        // 3. Execute query.
        IExecutionResult result = this.serviceExecutorContext.getPipelineRunner()
            .runPipelineGraph(pipelineGraph, serviceExecutorContext);
        return result;
    }
}
