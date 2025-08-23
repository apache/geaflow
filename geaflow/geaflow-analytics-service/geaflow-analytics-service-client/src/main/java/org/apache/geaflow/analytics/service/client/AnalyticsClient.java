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

import static org.apache.geaflow.analytics.service.config.AnalyticsClientConfigKeys.ANALYTICS_CLIENT_EXECUTE_RETRY_NUM;
import static org.apache.geaflow.analytics.service.config.AnalyticsClientConfigKeys.ANALYTICS_CLIENT_SLEEP_TIME_MS;
import static org.apache.geaflow.analytics.service.query.StandardError.ANALYTICS_NULL_RESULT;
import static org.apache.geaflow.analytics.service.query.StandardError.ANALYTICS_SERVER_BUSY;
import static org.apache.geaflow.analytics.service.query.StandardError.ANALYTICS_SERVER_UNAVAILABLE;

import com.google.common.base.Preconditions;
import org.apache.geaflow.analytics.service.client.QueryRunnerContext.ClientHandlerContextBuilder;
import org.apache.geaflow.analytics.service.query.QueryError;
import org.apache.geaflow.analytics.service.query.QueryResults;
import org.apache.geaflow.common.config.ConfigKey;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.common.config.keys.ExecutionConfigKeys;
import org.apache.geaflow.common.exception.GeaflowRuntimeException;
import org.apache.geaflow.common.mode.JobMode;
import org.apache.geaflow.common.rpc.HostAndPort;
import org.apache.geaflow.common.utils.SleepUtils;
import org.apache.geaflow.pipeline.service.ServiceType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AnalyticsClient {
    private static final Logger LOGGER = LoggerFactory.getLogger(AnalyticsClient.class);
    private final Configuration config;
    private final String host;
    private final int port;
    private final ServiceType serviceType;
    private final boolean initChannelPools;
    private final int executeRetryNum;
    private IQueryRunner queryRunner;
    private final long sleepTimeMs;

    public static AnalyticsClientBuilder builder() {
        return new AnalyticsClientBuilder();
    }

    protected AnalyticsClient(AnalyticsClientBuilder builder) {
        this.config = builder.getConfiguration();
        this.host = builder.getHost();
        this.port = builder.getPort();
        this.initChannelPools = builder.enableInitChannelPools();
        this.executeRetryNum = config.getInteger(ANALYTICS_CLIENT_EXECUTE_RETRY_NUM);
        this.sleepTimeMs = config.getLong(ANALYTICS_CLIENT_SLEEP_TIME_MS);
        this.serviceType = ServiceType.getEnum(config);
        init();
    }

    private void init() {
        if (this.host == null) {
            checkAnalyticsClientConfig(config);
        }
        ClientHandlerContextBuilder clientHandlerContextBuilder = QueryRunnerContext.newBuilder()
            .setConfiguration(config)
            .enableInitChannelPools(initChannelPools);
        if (host != null) {
            clientHandlerContextBuilder.setHost(new HostAndPort(host, port));
        }
        QueryRunnerContext clientHandlerContext = clientHandlerContextBuilder.build();
        this.queryRunner = QueryRunnerFactory.loadQueryRunner(clientHandlerContext);
    }


    public QueryResults executeQuery(String queryScript) {
        QueryResults result = null;
        for (int i = 0; i < this.executeRetryNum; i++) {
            result = this.queryRunner.executeQuery(queryScript);
            boolean serviceBusy = false;
            boolean serviceUnavailable = false;
            if (result.getError() != null) {
                int resultErrorCode = result.getError().getCode();
                serviceBusy = resultErrorCode == ANALYTICS_SERVER_BUSY.getQueryError().getCode();
                serviceUnavailable = resultErrorCode == ANALYTICS_SERVER_UNAVAILABLE.getQueryError().getCode();
            }
            if (result.getQueryStatus() || (!serviceBusy && !serviceUnavailable)) {
                return result;
            }
            LOGGER.info("all coordinator busy or unavailable, sleep {}ms and retry", sleepTimeMs);
            SleepUtils.sleepMilliSecond(sleepTimeMs);
        }
        if (result == null) {
            QueryError queryError = ANALYTICS_NULL_RESULT.getQueryError();
            return new QueryResults(queryError);
        }
        return result;
    }

    public void shutdown() {
        try {
            queryRunner.close();
        } catch (Throwable e) {
            LOGGER.error("client handler close error", e);
        }
    }

    protected static void checkAnalyticsClientConfig(Configuration config) {
        // Check job mode.
        checkAnalyticsClientJobMode(config);
    }

    private static void checkAnalyticsClientJobMode(Configuration config) {
        if (config.contains(ExecutionConfigKeys.JOB_MODE)) {
            JobMode jobMode = JobMode.getJobMode(config);
            Preconditions.checkArgument(JobMode.OLAP_SERVICE.equals(jobMode), "analytics job mode must set OLAP_SERVICE");
            return;
        }
        throw new GeaflowRuntimeException("analytics client config miss: " + ExecutionConfigKeys.JOB_MODE.getKey());
    }

    private static void configIsExist(Configuration config, ConfigKey configKey) {
        Preconditions.checkArgument(
            config.contains(configKey) && !config.getConfigMap().get(configKey.getKey()).isEmpty(),
            "client missing config: " + configKey.getKey() + ", description: "
                + configKey.getDescription());
    }

}
