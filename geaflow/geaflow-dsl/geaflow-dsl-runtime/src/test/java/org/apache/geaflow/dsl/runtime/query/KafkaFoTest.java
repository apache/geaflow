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

package org.apache.geaflow.dsl.runtime.query;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.geaflow.common.config.keys.DSLConfigKeys;
import org.apache.geaflow.common.utils.DateTimeUtil;
import org.apache.geaflow.dsl.connector.api.util.ConnectorConstants;
import org.apache.geaflow.dsl.runtime.testenv.KafkaTestEnv;
import org.apache.geaflow.dsl.runtime.testenv.SourceFunctionNoPartitionCheck;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class KafkaFoTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaFoTest.class);

    public static int injectExceptionTimes = 0;

    @BeforeClass
    public void startKafkaServer() throws IOException {
        LOGGER.info("startKafkaServer");
        KafkaTestEnv.get().startKafkaServer();
        LOGGER.info("startKafkaServer done");
    }

    @AfterClass
    public void shutdownKafkaServer() throws IOException {
        LOGGER.info("shutdownKafkaServer");
        KafkaTestEnv.get().shutdownKafkaServer();
        LOGGER.info("shutdownKafkaServer done");
    }

    @Test
    public void testKafka_001() throws Exception {
        Map<String, String> config = new HashMap<>();
        config.put(DSLConfigKeys.GEAFLOW_DSL_WINDOW_SIZE.getKey(), String.valueOf(1L));
        config.put(DSLConfigKeys.GEAFLOW_DSL_CUSTOM_SOURCE_FUNCTION.getKey(),
            SourceFunctionNoPartitionCheck.class.getName());
        KafkaTestEnv.get().createTopic("sink-test");
        QueryTester
            .build()
            .withQueryPath("/query/kafka_write_001.sql")
            .withConfig(config)
            .withTestTimeWaitSeconds(60)
            .execute();
        QueryTester tester = QueryTester
            .build()
            .withQueryPath("/query/kafka_scan_001.sql")
            .withConfig(config)
            .withTestTimeWaitSeconds(60);
        try {
            tester.execute();
        } catch (Exception e) {
            LOGGER.info("Kafka unbounded stream finish with timeout.");
        }
        tester.checkSinkResult();
    }

    @Test
    public void testKafka_002() throws Exception {
        Map<String, String> config = new HashMap<>();
        config.put(DSLConfigKeys.GEAFLOW_DSL_CUSTOM_SOURCE_FUNCTION.getKey(),
            SourceFunctionNoPartitionCheck.class.getName());
        String startTime = DateTimeUtil.fromUnixTime(System.currentTimeMillis() - 120 * 1000, ConnectorConstants.START_TIME_FORMAT);
        config.put("startTime", startTime);
        KafkaTestEnv.get().createTopic("scan_002");
        QueryTester
            .build()
            .withQueryPath("/query/kafka_write_002.sql")
            .withConfig(config)
            .withTestTimeWaitSeconds(60)
            .execute();
        QueryTester tester = QueryTester
            .build()
            .withQueryPath("/query/kafka_scan_002.sql")
            .withCustomWindow()
            .withConfig(config)
            .withTestTimeWaitSeconds(60);
        try {
            tester.execute();
        } catch (Exception e) {
            LOGGER.info("Kafka unbounded stream finish with timeout.");
        }
        tester.checkSinkResult();
    }

}
