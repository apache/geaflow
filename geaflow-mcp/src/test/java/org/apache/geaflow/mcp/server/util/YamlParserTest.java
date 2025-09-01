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

package org.apache.geaflow.mcp.server.util;

import static org.apache.geaflow.mcp.server.GeaFlowMcpServerTools.CONFIG;
import static org.apache.geaflow.mcp.server.GeaFlowMcpServerTools.SERVER_HOST;
import static org.apache.geaflow.mcp.server.GeaFlowMcpServerTools.SERVER_PORT;

import com.alibaba.fastjson.JSON;
import java.util.Map;
import org.apache.geaflow.analytics.service.config.AnalyticsClientConfigKeys;
import org.apache.geaflow.mcp.util.YamlParser;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class YamlParserTest {

    private static final String LOCAL_HOST = "localhost";
    private static final String NOT_EXIST = "not-exist";
    private static final int LOCAL_SERVER_PORT = 8090;
    private static final int DEFAULT_MESSAGE_SIZE = 4194304;

    @Test
    public void testLoadConfig() {
        Map<String, Object> config = YamlParser.loadConfig();
        Assertions.assertNotNull(config);

        Assertions.assertTrue(config.containsKey(SERVER_HOST));
        Assertions.assertEquals(LOCAL_HOST, config.get(SERVER_HOST));

        Assertions.assertTrue(config.containsKey(SERVER_PORT));
        Assertions.assertEquals(LOCAL_SERVER_PORT, (int) config.get(SERVER_PORT));

        Assertions.assertFalse(config.containsKey(NOT_EXIST));

        Assertions.assertTrue(config.containsKey(CONFIG));
        Map<String, Object> clientConfig = JSON.parseObject(config.get(CONFIG).toString(), Map.class);
        String key = AnalyticsClientConfigKeys.ANALYTICS_CLIENT_MAX_INBOUND_MESSAGE_SIZE.getKey();
        Assertions.assertTrue(clientConfig.containsKey(key));
        Assertions.assertEquals(DEFAULT_MESSAGE_SIZE, clientConfig.get(key));
    }
}
