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

import org.apache.geaflow.analytics.service.config.AnalyticsClientConfigKeys;
import org.apache.geaflow.common.config.Configuration;
import org.testng.Assert;
import org.testng.annotations.Test;

public class AnalyticsClientBuilderTest {

    @Test
    public void testBuilderWithHost() {
        AnalyticsClientBuilder builder = new AnalyticsClientBuilder();
        builder.withHost("localhost");
        Assert.assertEquals(builder.getHost(), "localhost");
    }

    @Test
    public void testBuilderWithPort() {
        AnalyticsClientBuilder builder = new AnalyticsClientBuilder();
        builder.withPort(8080);
        Assert.assertEquals(builder.getPort(), 8080);
    }

    @Test
    public void testBuilderWithUser() {
        AnalyticsClientBuilder builder = new AnalyticsClientBuilder();
        builder.withUser("testUser");
        Assert.assertEquals(builder.getUser(), "testUser");
    }

    @Test
    public void testBuilderWithTimeoutMs() {
        AnalyticsClientBuilder builder = new AnalyticsClientBuilder();
        builder.withTimeoutMs(5000);
        Assert.assertEquals(
            builder.getConfiguration().getString(AnalyticsClientConfigKeys.ANALYTICS_CLIENT_CONNECT_TIMEOUT_MS),
            "5000"
        );
    }

    @Test
    public void testBuilderWithRetryNum() {
        AnalyticsClientBuilder builder = new AnalyticsClientBuilder();
        builder.withRetryNum(3);
        Assert.assertEquals(builder.getQueryRetryNum(), 3);
        Assert.assertEquals(
            builder.getConfiguration().getString(AnalyticsClientConfigKeys.ANALYTICS_CLIENT_CONNECT_RETRY_NUM),
            "3"
        );
    }

    @Test
    public void testBuilderWithInitChannelPools() {
        AnalyticsClientBuilder builder = new AnalyticsClientBuilder();
        builder.withInitChannelPools(true);
        Assert.assertTrue(builder.enableInitChannelPools());
    }

    @Test
    public void testBuilderWithConfiguration() {
        Configuration config = new Configuration();
        config.put("test.key", "test.value");
        
        AnalyticsClientBuilder builder = new AnalyticsClientBuilder();
        builder.withConfiguration(config);
        
        Assert.assertEquals(builder.getConfiguration().getString("test.key"), "test.value");
    }

    @Test
    public void testBuilderChaining() {
        AnalyticsClientBuilder builder = new AnalyticsClientBuilder()
            .withHost("localhost")
            .withPort(8080)
            .withUser("testUser")
            .withTimeoutMs(5000)
            .withRetryNum(3)
            .withInitChannelPools(true);
        
        Assert.assertEquals(builder.getHost(), "localhost");
        Assert.assertEquals(builder.getPort(), 8080);
        Assert.assertEquals(builder.getUser(), "testUser");
        Assert.assertEquals(builder.getQueryRetryNum(), 3);
        Assert.assertTrue(builder.enableInitChannelPools());
    }
}
