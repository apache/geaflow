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


import static org.apache.geaflow.analytics.service.config.AnalyticsServiceConfigKeys.ANALYTICS_COMPILE_SCHEMA_ENABLE;
import static org.apache.geaflow.analytics.service.config.AnalyticsServiceConfigKeys.ANALYTICS_QUERY;
import static org.apache.geaflow.analytics.service.config.AnalyticsServiceConfigKeys.ANALYTICS_SERVICE_REGISTER_ENABLE;

import com.github.fppt.jedismock.RedisServer;
import java.io.IOException;
import java.util.List;
import org.apache.geaflow.analytics.service.client.AnalyticsClient;
import org.apache.geaflow.analytics.service.config.AnalyticsServiceConfigKeys;
import org.apache.geaflow.analytics.service.query.QueryResults;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.common.config.keys.ExecutionConfigKeys;
import org.apache.geaflow.common.config.keys.FrameworkConfigKeys;
import org.apache.geaflow.common.mode.JobMode;
import org.apache.geaflow.common.utils.SleepUtils;
import org.apache.geaflow.env.EnvironmentFactory;
import org.apache.geaflow.example.base.BaseTest;
import org.apache.geaflow.store.redis.RedisConfigKeys;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class AnalyticsClientTest extends BaseTest {

    private static final String QUERY = "hello world";
    private static final String HOST_NAME = "localhost";
    private static final int DEFAULT_WAITING_TIME = 5;

    @BeforeMethod
    public void setUp() {
        config.put(ANALYTICS_COMPILE_SCHEMA_ENABLE.getKey(), String.valueOf(false));
        config.put(ANALYTICS_SERVICE_REGISTER_ENABLE.getKey(), Boolean.FALSE.toString());
        config.put(ANALYTICS_QUERY.getKey(), QUERY);
    }

    @Test
    public void testWordLengthWithAnalyticsClientByRpc() {
        int testPort = 8091;
        environment = EnvironmentFactory.onLocalEnvironment();
        Configuration configuration = environment.getEnvironmentContext().getConfig();
        configuration.putAll(config);
        configuration.put(AnalyticsServiceConfigKeys.ANALYTICS_SERVICE_PORT, String.valueOf(testPort));
        configuration.put(FrameworkConfigKeys.SERVICE_SERVER_TYPE, "analytics_rpc");
        WordLengthService wordLengthService = new WordLengthService();
        wordLengthService.submit(environment);
        SleepUtils.sleepSecond(DEFAULT_WAITING_TIME);

        AnalyticsClient analyticsClient = AnalyticsClient.builder()
            .withHost(HOST_NAME)
            .withPort(testPort)
            .withConfiguration(configuration)
            .withRetryNum(3)
            .build();

        QueryResults queryResults = analyticsClient.executeQuery(QUERY);
        Assert.assertNotNull(queryResults);
        List<List<Object>> rawData = queryResults.getRawData();
        Assert.assertEquals(rawData.size(), 1);
        Assert.assertEquals((int) rawData.get(0).get(0), 11);
        analyticsClient.shutdown();
    }

    @Test
    public void testWordLengthWithAnalyticsClientByHttp() {
        int testPort = 8092;
        environment = EnvironmentFactory.onLocalEnvironment();
        Configuration configuration = environment.getEnvironmentContext().getConfig();
        configuration.putAll(config);
        configuration.put(AnalyticsServiceConfigKeys.ANALYTICS_SERVICE_PORT, String.valueOf(testPort));
        configuration.put(FrameworkConfigKeys.SERVICE_SERVER_TYPE, "analytics_http");
        environment.getEnvironmentContext().withConfig(configuration.getConfigMap());
        WordLengthService wordLengthService = new WordLengthService();
        wordLengthService.submit(environment);
        SleepUtils.sleepSecond(DEFAULT_WAITING_TIME);

        AnalyticsClient analyticsClient = AnalyticsClient.builder()
            .withHost(HOST_NAME)
            .withPort(testPort)
            .withConfiguration(configuration)
            .withRetryNum(3)
            .build();

        QueryResults queryResults = analyticsClient.executeQuery(QUERY);
        Assert.assertNotNull(queryResults);
        List<List<Object>> rawData = queryResults.getRawData();
        Assert.assertEquals(rawData.size(), 1);
        Assert.assertEquals((int) rawData.get(0).get(0), 11);
        analyticsClient.shutdown();
    }

    @Test
    public void testWordLengthWithRedisMetaService() throws IOException {
        RedisServer redisServer = null;
        AnalyticsClient analyticsClient = null;
        try {
            redisServer = RedisServer.newRedisServer().start();
            environment = EnvironmentFactory.onLocalEnvironment();
            Configuration configuration = environment.getEnvironmentContext().getConfig();
            configuration.putAll(config);
            configuration.put(AnalyticsServiceConfigKeys.ANALYTICS_SERVICE_REGISTER_ENABLE, Boolean.TRUE.toString());
            configuration.put(ANALYTICS_COMPILE_SCHEMA_ENABLE, Boolean.FALSE.toString());
            configuration.put(ExecutionConfigKeys.JOB_MODE.getKey(), JobMode.OLAP_SERVICE.toString());
            configuration.put(ExecutionConfigKeys.SERVICE_DISCOVERY_TYPE, "redis");
            configuration.put(FrameworkConfigKeys.SERVICE_SERVER_TYPE, "analytics_rpc");
            configuration.put(RedisConfigKeys.REDIS_HOST, redisServer.getHost());
            configuration.put(RedisConfigKeys.REDIS_PORT, String.valueOf(redisServer.getBindPort()));

            WordLengthService wordLengthService = new WordLengthService();
            wordLengthService.submit(environment);
            SleepUtils.sleepSecond(DEFAULT_WAITING_TIME);

            analyticsClient = AnalyticsClient.builder()
                .withRetryNum(3)
                .withConfiguration(configuration)
                .build();

            QueryResults queryResults = analyticsClient.executeQuery(QUERY);
            Assert.assertNotNull(queryResults);
            List<List<Object>> list = queryResults.getRawData();
            Assert.assertEquals(list.size(), 1);
            Assert.assertEquals((int) list.get(0).get(0), 11);
        } finally {
            if (redisServer != null) {
                redisServer.stop();
            }
            if (analyticsClient != null) {
                analyticsClient.shutdown();
            }
        }
    }
}
