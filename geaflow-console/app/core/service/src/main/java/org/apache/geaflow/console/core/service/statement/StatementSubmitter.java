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

package org.apache.geaflow.console.core.service.statement;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.geaflow.console.common.service.integration.engine.analytics.AnalyticsClient;
import org.apache.geaflow.console.common.service.integration.engine.analytics.QueryResults;
import org.apache.geaflow.console.common.util.context.ContextHolder;
import org.apache.geaflow.console.common.util.type.GeaflowStatementStatus;
import org.apache.geaflow.console.core.model.data.GeaflowGraph;
import org.apache.geaflow.console.core.model.statement.GeaflowStatement;
import org.apache.geaflow.console.core.model.task.GeaflowTask;
import org.apache.geaflow.console.core.service.StatementService;
import org.apache.geaflow.console.core.service.version.VersionFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class StatementSubmitter {

    private static final ExecutorService EXECUTOR_SERVICE = new ThreadPoolExecutor(10, 30,
        30, TimeUnit.SECONDS, new LinkedBlockingQueue<>(100));

    private static final String queryTemplate = "USE GRAPH %s; \n %s;";
    @Autowired
    private VersionFactory versionFactory;

    @Autowired
    private AnalyticsClientPool analyticsClientPool;

    @Autowired
    private StatementService statementService;

    public void asyncSubmitQuery(GeaflowStatement query, GeaflowTask task) {
        final String sessionToken = ContextHolder.get().getSessionToken();
        EXECUTOR_SERVICE.submit(() -> {
            try {
                ContextHolder.init();
                ContextHolder.get().setSessionToken(sessionToken);

                submitQuery(query, task);

            } finally {
                ContextHolder.destroy();
            }
        });
    }

    private void submitQuery(GeaflowStatement query, GeaflowTask task) {
        GeaflowStatementStatus status = null;
        String result = null;
        AnalyticsClient client = null;
        try {
            String script = formatQuery(query.getScript(), task);
            client = analyticsClientPool.getClient(task);
            QueryResults queryResults = client.executeQuery(script);
            if (queryResults.getQueryStatus()) {
                status = GeaflowStatementStatus.FINISHED;
                result = queryResults.getFormattedData();
            } else {
                status = GeaflowStatementStatus.FAILED;
                result = queryResults.getError().getName();
            }

        } catch (Exception e) {
            status = GeaflowStatementStatus.FAILED;
            result = ExceptionUtils.getStackTrace(e);

        } finally {
            log.info("query finish {}, {}, {}", query.getScript(), status, result);
            query.setStatus(status);
            query.setResult(result);
            statementService.update(query);
            if (client != null) {
                analyticsClientPool.addClient(task, client);
            }

        }
    }

    private String formatQuery(String script, GeaflowTask task) {
        GeaflowGraph graph = task.getRelease().getJob().getGraphs().get(0);
        return String.format(queryTemplate, graph.getName(), script);
    }
}
