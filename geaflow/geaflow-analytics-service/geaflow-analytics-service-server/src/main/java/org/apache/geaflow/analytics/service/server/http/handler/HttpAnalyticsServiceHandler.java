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

package org.apache.geaflow.analytics.service.server.http.handler;

import static org.apache.geaflow.analytics.service.server.AbstractAnalyticsServiceServer.getQueryResults;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import java.io.IOException;
import java.lang.reflect.Type;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.stream.Collectors;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.geaflow.analytics.service.query.QueryError;
import org.apache.geaflow.analytics.service.query.QueryIdGenerator;
import org.apache.geaflow.analytics.service.query.QueryInfo;
import org.apache.geaflow.analytics.service.query.QueryResults;
import org.apache.geaflow.analytics.service.query.StandardError;
import org.apache.geaflow.common.blocking.map.BlockingMap;
import org.apache.geaflow.common.serialize.SerializerFactory;
import org.apache.geaflow.runtime.core.scheduler.result.IExecutionResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HttpAnalyticsServiceHandler extends AbstractHttpHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(HttpAnalyticsServiceHandler.class);
    private static final Type DEFAULT_REQUEST_TYPE = new TypeToken<Map<String, Object>>() {
    }.getType();
    private static final String QUERY = "query";

    private final BlockingQueue<QueryInfo> requestBlockingQueue;
    private final BlockingMap<String, Future<IExecutionResult>> responseBlockingMap;
    private final QueryIdGenerator queryIdGenerator;
    private final Semaphore semaphore;

    public HttpAnalyticsServiceHandler(BlockingQueue<QueryInfo> requestBlockingQueue,
                                       BlockingMap<String, Future<IExecutionResult>> responseBlockingMap,
                                       Semaphore semaphore) {
        this.requestBlockingQueue = requestBlockingQueue;
        this.responseBlockingMap = responseBlockingMap;
        this.semaphore = semaphore;
        this.queryIdGenerator = new QueryIdGenerator();
    }

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws IOException {
        QueryResults result = null;
        String queryId = queryIdGenerator.createQueryId();
        try {
            if (!this.semaphore.tryAcquire()) {
                QueryError queryError = StandardError.ANALYTICS_SERVER_BUSY.getQueryError();
                result = new QueryResults(queryId, queryError);
                resp.setStatus(HttpServletResponse.SC_OK);
            } else {
                String requestBody = req.getReader().lines().collect(Collectors.joining(System.lineSeparator()));
                Map<String, Object> requestParam = new Gson().fromJson(requestBody, DEFAULT_REQUEST_TYPE);
                String query = requestParam.get(QUERY).toString();
                QueryInfo queryInfo = new QueryInfo(queryId, query);
                LOGGER.info("start execute query [{}]", queryInfo);
                final long start = System.currentTimeMillis();
                requestBlockingQueue.put(queryInfo);
                result = getQueryResults(queryInfo, responseBlockingMap);
                LOGGER.info("finish execute query [{}], result {}, cost {}ms", result, resp, System.currentTimeMillis() - start);
                resp.setStatus(HttpServletResponse.SC_OK);
            }
        } catch (Throwable t) {
            result = new QueryResults(queryId, new QueryError(t.getMessage()));
            resp.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
        } finally {
            addHeader(resp);
            byte[] serializeResult = SerializerFactory.getKryoSerializer().serialize(result);
            resp.getOutputStream().write(serializeResult);
            resp.getOutputStream().flush();
            this.semaphore.release();
        }
    }

}
