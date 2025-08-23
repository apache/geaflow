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

package org.apache.geaflow.analytics.service.config;

import java.io.Serializable;
import org.apache.geaflow.common.config.ConfigKey;
import org.apache.geaflow.common.config.ConfigKeys;

public class AnalyticsClientConfigKeys implements Serializable {

    public static final ConfigKey ANALYTICS_CLIENT_CONNECT_TIMEOUT_MS = ConfigKeys
        .key("geaflow.analytics.client.connect.timeout.ms")
        .defaultValue(30000)
        .description("analytics client connect timeout ms, default is 30000");

    public static final ConfigKey ANALYTICS_CLIENT_REQUEST_TIMEOUT_MS = ConfigKeys
        .key("geaflow.analytics.client.request.timeout.ms")
        .defaultValue(30000)
        .description("analytics client request timeout ms, default is 30000");

    public static final ConfigKey ANALYTICS_CLIENT_CONNECT_RETRY_NUM = ConfigKeys
        .key("geaflow.analytics.client.connect.retry.num")
        .defaultValue(3)
        .description("analytics client connect retry num, default is 3");

    public static final ConfigKey ANALYTICS_CLIENT_EXECUTE_RETRY_NUM = ConfigKeys
        .key("geaflow.analytics.client.execute.retry.num")
        .defaultValue(3)
        .description("analytics client execute retry num, default is 3");

    public static final ConfigKey ANALYTICS_CLIENT_MAX_INBOUND_MESSAGE_SIZE = ConfigKeys
        .key("geaflow.analytics.client.max.inbound.message.size")
        .defaultValue(4194304)
        .description("analytics client max inbound message size for rpc, default is 4194304");

    public static final ConfigKey ANALYTICS_CLIENT_MAX_RETRY_ATTEMPTS = ConfigKeys
        .key("geaflow.analytics.client.max.retry.attempts")
        .defaultValue(5)
        .description("analytics client max retry attempts for rpc, default is 5");

    public static final ConfigKey ANALYTICS_CLIENT_DEFALUT_RETRY_BUFFER_SIZE = ConfigKeys
        .key("geaflow.analytics.client.retry.buffer.size")
        .defaultValue(16777216L)
        .description("analytics client default retry buffer size for rpc, default is 16777216");

    public static final ConfigKey ANALYTICS_CLIENT_PER_RPC_BUFFER_LIMIT = ConfigKeys
        .key("geaflow.analytics.client.per.rpc.buffer.limit")
        .defaultValue(1048576L)
        .description("analytics client per rpc buffer limit, default is 1048576");

    public static final ConfigKey ANALYTICS_CLIENT_ACCESS_TOKEN = ConfigKeys
        .key("geaflow.analytics.client.access.token")
        .noDefaultValue()
        .description("analytics client access token for auth");

    public static final ConfigKey ANALYTICS_CLIENT_SLEEP_TIME_MS = ConfigKeys
        .key("geaflow.analytics.client.sleep.time.ms")
        .defaultValue(3000L)
        .description("analytics client sleep time");
}
