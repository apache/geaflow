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

package org.apache.geaflow.example.service;

import com.google.common.base.Preconditions;
import org.apache.geaflow.analytics.service.config.AnalyticsServiceConfigKeys;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.dsl.runtime.QueryClient;
import org.apache.geaflow.dsl.runtime.QueryContext;
import org.apache.geaflow.dsl.runtime.QueryEngine;
import org.apache.geaflow.dsl.runtime.engine.GeaFlowQueryEngine;
import org.apache.geaflow.env.Environment;
import org.apache.geaflow.example.util.EnvironmentUtil;
import org.apache.geaflow.pipeline.IPipelineResult;
import org.apache.geaflow.pipeline.Pipeline;
import org.apache.geaflow.pipeline.PipelineFactory;
import org.apache.geaflow.pipeline.service.PipelineService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QueryService {

    private static final Logger LOGGER = LoggerFactory.getLogger(QueryService.class);

    private static final String WARM_UP_PATTERN = "USE GRAPH %s ; MATCH (a) RETURN a limit 0";

    public static void main(String[] args) {
        Environment environment = EnvironmentUtil.loadEnvironment(args);
        Configuration configuration = environment.getEnvironmentContext().getConfig();

        String graphViewName = configuration.getConfigMap().get("geaflow.analytics.graph.view.name");
        Preconditions.checkNotNull(graphViewName, "graph view name is null");
        configuration.put(AnalyticsServiceConfigKeys.ANALYTICS_QUERY, String.format(WARM_UP_PATTERN, graphViewName));
        submit(environment);
        LOGGER.info("query service start finish");
        synchronized (QueryService.class) {
            try {
                QueryService.class.wait();
            } catch (Throwable e) {
                LOGGER.error("wait server failed");
            }
        }
    }

    public static IPipelineResult submit(Environment environment) {
        Configuration configuration = environment.getEnvironmentContext().getConfig();
        Pipeline pipeline = PipelineFactory.buildPipeline(environment);
        pipeline.start((PipelineService) pipelineServiceContext -> {
            QueryClient queryManager = new QueryClient();
            QueryEngine engineContext = new GeaFlowQueryEngine(pipelineServiceContext);
            QueryContext queryContext = QueryContext.builder()
                .setEngineContext(engineContext)
                .setTraversalParallelism(configuration.getInteger(AnalyticsServiceConfigKeys.ANALYTICS_QUERY_PARALLELISM))
                .setCompile(false)
                .build();
            queryManager.executeQuery((String) pipelineServiceContext.getRequest(), queryContext);
        });
        return pipeline.execute();
    }
}