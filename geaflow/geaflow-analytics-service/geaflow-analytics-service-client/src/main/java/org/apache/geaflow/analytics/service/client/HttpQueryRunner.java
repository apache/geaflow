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

package org.apache.geaflow.analytics.service.client;

import static com.google.common.base.MoreObjects.firstNonNull;
import static org.apache.geaflow.analytics.service.client.AnalyticsManagerOptions.createClientSession;
import static org.apache.geaflow.analytics.service.client.QueryRunnerStatus.ABORTED;
import static org.apache.geaflow.analytics.service.client.QueryRunnerStatus.ERROR;
import static org.apache.geaflow.analytics.service.client.QueryRunnerStatus.FINISHED;
import static org.apache.geaflow.analytics.service.client.QueryRunnerStatus.RUNNING;
import static org.apache.geaflow.analytics.service.config.AnalyticsClientConfigKeys.ANALYTICS_CLIENT_CONNECT_RETRY_NUM;
import static org.apache.geaflow.analytics.service.config.AnalyticsClientConfigKeys.ANALYTICS_CLIENT_CONNECT_TIMEOUT_MS;
import static org.apache.geaflow.analytics.service.config.AnalyticsClientConfigKeys.ANALYTICS_CLIENT_REQUEST_TIMEOUT_MS;
import static org.apache.geaflow.analytics.service.query.StandardError.ANALYTICS_NO_COORDINATOR;
import static org.apache.geaflow.analytics.service.query.StandardError.ANALYTICS_SERVER_BUSY;
import static org.apache.http.HttpStatus.SC_OK;

import com.google.gson.Gson;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;
import org.apache.geaflow.analytics.service.query.QueryError;
import org.apache.geaflow.analytics.service.query.QueryResults;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.common.errorcode.RuntimeErrors;
import org.apache.geaflow.common.exception.GeaflowRuntimeException;
import org.apache.geaflow.common.rpc.HostAndPort;
import org.apache.geaflow.pipeline.service.ServiceType;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.config.SocketConfig;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.DefaultHttpRequestRetryHandler;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.LaxRedirectStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HttpQueryRunner extends AbstractQueryRunner {

    private static final Logger LOGGER = LoggerFactory.getLogger(HttpQueryRunner.class);
    private static final String USER_AGENT_VALUE = HttpQueryRunner.class.getSimpleName() + "/" + firstNonNull(
        HttpQueryRunner.class.getPackage().getImplementationVersion(), "unknown");

    protected static final String URL_PATH = "/rest/analytics/query/execute";
    protected static final String QUERY = "query";
    private Map<HostAndPort, CloseableHttpClient> coordinatorAddress2Stub;

    @Override
    public void init(QueryRunnerContext context) {
        this.coordinatorAddress2Stub = new HashMap<>();
        super.init(context);
    }

    @Override
    protected void initManagedChannel(HostAndPort address) {
        CloseableHttpClient httpClient = this.coordinatorAddress2Stub.get(address);
        if (httpClient == null) {
            this.coordinatorAddress2Stub.put(address, createHttpClient(config));
        }
    }

    @Override
    public QueryResults executeQuery(String queryScript) {
        int coordinatorNum = analyticsServiceInfo.getCoordinatorNum();
        if (coordinatorNum == 0) {
            QueryError queryError = ANALYTICS_NO_COORDINATOR.getQueryError();
            return new QueryResults(queryError);
        }
        int idx = RANDOM.nextInt(coordinatorNum);
        List<HostAndPort> coordinatorAddresses = analyticsServiceInfo.getCoordinatorAddresses();
        QueryResults result = null;
        for (int i = 0; i < coordinatorAddresses.size(); i++) {
            HostAndPort address = coordinatorAddresses.get(idx);
            final long start = System.currentTimeMillis();
            result = executeInternal(address, queryScript);
            LOGGER.info("coordinator {} execute query script {} finish, cost {} ms", address, queryScript, System.currentTimeMillis() - start);
            if (!result.getQueryStatus() && result.getError().getCode() == ANALYTICS_SERVER_BUSY.getQueryError().getCode()) {
                LOGGER.warn("coordinator[{}] [{}] is busy, try next", idx, address.toString());
                idx = (idx + 1) % coordinatorNum;
                continue;
            }
            queryRunnerStatus.compareAndSet(RUNNING, FINISHED);
            return result;
        }

        if (result != null && (!result.getQueryStatus() && result.getError().getCode() == ANALYTICS_SERVER_BUSY.getQueryError().getCode())) {
            QueryError queryError = ANALYTICS_SERVER_BUSY.getQueryError();
            LOGGER.error(queryError.getName());
            queryRunnerStatus.compareAndSet(RUNNING, ERROR);
            return new QueryResults(queryError);
        }
        throw new GeaflowRuntimeException(RuntimeErrors.INST.analyticsClientError(String.format("execute query [%s] error", queryScript)));
    }

    @Override
    public ServiceType getServiceType() {
        return ServiceType.analytics_http;
    }

    @Override
    public QueryResults cancelQuery(long queryId) {
        throw new GeaflowRuntimeException("not support cancel query");
    }


    private QueryResults executeInternal(HostAndPort address, String script) {
        CloseableHttpClient httpClient = coordinatorAddress2Stub.get(address);
        if (httpClient == null) {
            initManagedChannel(address);
            httpClient = coordinatorAddress2Stub.get(address);
        }
        AnalyticsManagerSession analyticsManagerSession = createClientSession(address.getHost(), address.getPort());
        HttpUriRequest queryRequest = buildQueryRequest(analyticsManagerSession, script);
        HttpResponse response = HttpResponse.execute(httpClient, queryRequest);
        if ((response.getStatusCode() != SC_OK) || !response.enableQuerySuccess()) {
            this.initAnalyticsServiceAddress();
            queryRunnerStatus.compareAndSet(RUNNING, ABORTED);
            LOGGER.warn("coordinator execute query error, need re-init");
        }
        return response.getValue();
    }

    @Override
    public void close() throws IOException {
        for (Entry<HostAndPort, CloseableHttpClient> entry : this.coordinatorAddress2Stub.entrySet()) {
            HostAndPort address = entry.getKey();
            CloseableHttpClient client = entry.getValue();
            if (client != null) {
                try {
                    client.close();
                } catch (Exception e) {
                    LOGGER.warn("coordinator [{}:{}] shutdown failed", address.getHost(),
                        address.getPort(), e);
                    throw new GeaflowRuntimeException(String.format("coordinator [%s:%d] "
                        + "shutdown error", address.getHost(), address.getPort()), e);
                }
            }
        }
        if (this.serverQueryClient != null) {
            this.serverQueryClient.close();
        }
        LOGGER.info("http query executor shutdown");
    }

    private CloseableHttpClient createHttpClient(Configuration config) {
        HttpClientBuilder clientBuilder = HttpClientBuilder.create();
        int connectTimeout = config.getInteger(ANALYTICS_CLIENT_CONNECT_TIMEOUT_MS);
        clientBuilder.setConnectionTimeToLive(connectTimeout, TimeUnit.MILLISECONDS);
        int retryNum = config.getInteger(ANALYTICS_CLIENT_CONNECT_RETRY_NUM);
        clientBuilder.setRetryHandler(new DefaultHttpRequestRetryHandler(retryNum, true));
        clientBuilder.setRedirectStrategy(new LaxRedirectStrategy());
        int requestTimeout = config.getInteger(ANALYTICS_CLIENT_REQUEST_TIMEOUT_MS);
        SocketConfig socketConfig = SocketConfig.custom()
            .setSoTimeout(requestTimeout)
            .setTcpNoDelay(true)
            .build();
        clientBuilder.setDefaultSocketConfig(socketConfig);
        clientBuilder.setUserAgent(USER_AGENT_VALUE);
        return clientBuilder.build();
    }


    private HttpUriRequest buildQueryRequest(AnalyticsManagerSession session, String script) {
        URI serverUri = session.getServer();
        if (serverUri == null) {
            throw new GeaflowRuntimeException("Invalid server URL is null");
        }
        String fullUri = serverUri.resolve(URL_PATH).toString();
        HttpPost httpPost = new HttpPost(fullUri);
        Map<String, Object> params = new HashMap<>();
        params.put(QUERY, script);
        StringEntity requestEntity = new StringEntity(new Gson().toJson(params), ContentType.APPLICATION_JSON);
        httpPost.setEntity(requestEntity);
        Map<String, String> customHeaders = session.getCustomHeaders();
        customHeaders.forEach(httpPost::setHeader);
        return httpPost;
    }

}
