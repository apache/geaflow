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

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.geaflow.analytics.service.client.AnalyticsClient;
import org.apache.geaflow.analytics.service.config.AnalyticsServiceConfigKeys;
import org.apache.geaflow.analytics.service.query.QueryResults;
import org.apache.geaflow.cluster.system.ClusterMetaStore;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.common.config.keys.ExecutionConfigKeys;
import org.apache.geaflow.common.utils.SleepUtils;
import org.apache.geaflow.env.EnvironmentFactory;
import org.apache.geaflow.example.base.BaseTest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class ConcurrencyWordLengthServiceTest extends BaseTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(ConcurrencyQueryServiceTest.class);
    private static final String QUERY = "hello world";
    private static final Integer THREAD_NUM = 2;
    private static final String TEST_HOST = "localhost";

    @BeforeMethod
    public void setUp() {
        config.put(ANALYTICS_COMPILE_SCHEMA_ENABLE.getKey(), String.valueOf(false));
        config.put(ANALYTICS_SERVICE_REGISTER_ENABLE.getKey(), Boolean.FALSE.toString());
        config.put(ANALYTICS_QUERY.getKey(), QUERY);
        ClusterMetaStore.close();
    }

    @Test
    public void testWordLengthWithHttpServer() throws Exception {
        int port = 8091;
        environment = EnvironmentFactory.onLocalEnvironment();
        Configuration configuration = environment.getEnvironmentContext().getConfig();
        configuration.put(AnalyticsServiceConfigKeys.ANALYTICS_SERVICE_PORT, String.valueOf(port));
        configuration.put(AnalyticsServiceConfigKeys.MAX_REQUEST_PER_SERVER, String.valueOf(THREAD_NUM));
        configuration.put(ExecutionConfigKeys.RPC_IO_THREAD_NUM, "32");
        configuration.put(ExecutionConfigKeys.RPC_WORKER_THREAD_NUM, "32");
        configuration.putAll(config);
        WordLengthService wordLengthService = new WordLengthService();
        wordLengthService.submit(environment);

        testHttpServiceServer(port);
    }

    private static void testHttpServiceServer(int port) throws Exception {
        SleepUtils.sleepSecond(3);

        List<CompletableFuture<QueryResults>> resultFutureList = new LinkedList<>();
        List<AnalyticsClient> clientLinkedList = new LinkedList<>();
        for (int i = 0; i < THREAD_NUM; i++) {
            AnalyticsClient client = AnalyticsClient.builder()
                .withHost(TEST_HOST)
                .withPort(port)
                .withRetryNum(3)
                .build();

            LOGGER.info("client {}: {}", i, client);
            clientLinkedList.add(client);
            resultFutureList.add(CompletableFuture.supplyAsync(() -> client.executeQuery(QUERY)));
        }

        LOGGER.info("resultFutureList: {}", resultFutureList);

        for (int i = 0; i < THREAD_NUM; i++) {
            CompletableFuture<QueryResults> future = resultFutureList.get(i);
            QueryResults queryResults = future.get();
            Assert.assertNotNull(queryResults);
            List<List<Object>> rawData = queryResults.getRawData();
            Assert.assertEquals(rawData.size(), 1);
            Assert.assertEquals((int) rawData.get(0).get(0), 11);
        }
        clientLinkedList.forEach(AnalyticsClient::shutdown);
    }
}
