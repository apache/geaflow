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

import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.common.rpc.HostAndPort;
import org.testng.Assert;
import org.testng.annotations.Test;

public class QueryRunnerContextTest {

    @Test
    public void testQueryRunnerContextBuilder() {
        Configuration config = new Configuration();
        config.put("test.key", "test.value");
        HostAndPort hostAndPort = new HostAndPort("localhost", 8080);

        QueryRunnerContext context = QueryRunnerContext.newBuilder()
            .setConfiguration(config)
            .setHost(hostAndPort)
            .enableInitChannelPools(true)
            .setAnalyticsServiceJobName("testJob")
            .setMetaServerAddress("meta-server:8081")
            .build();

        Assert.assertNotNull(context.getConfiguration());
        Assert.assertEquals(context.getConfiguration().getString("test.key"), "test.value");
        Assert.assertEquals(context.getHostAndPort(), hostAndPort);
        Assert.assertTrue(context.isInitChannelPools());
        Assert.assertTrue(context.enableInitChannelPools());
        Assert.assertEquals(context.getMetaServerBaseNode(), "testJob");
        Assert.assertEquals(context.getMetaServerAddress(), "meta-server:8081");
    }

    @Test
    public void testQueryRunnerContextBuilderWithMinimalParams() {
        Configuration config = new Configuration();
        
        QueryRunnerContext context = QueryRunnerContext.newBuilder()
            .setConfiguration(config)
            .build();

        Assert.assertNotNull(context.getConfiguration());
        Assert.assertNull(context.getHostAndPort());
        Assert.assertFalse(context.isInitChannelPools());
        Assert.assertNull(context.getMetaServerBaseNode());
        Assert.assertNull(context.getMetaServerAddress());
    }

    @Test
    public void testQueryRunnerContextBuilderChaining() {
        Configuration config = new Configuration();
        HostAndPort hostAndPort = new HostAndPort("host", 9090);

        QueryRunnerContext.ClientHandlerContextBuilder builder = QueryRunnerContext.newBuilder();
        QueryRunnerContext context = builder
            .setConfiguration(config)
            .setHost(hostAndPort)
            .enableInitChannelPools(false)
            .build();

        Assert.assertNotNull(context);
        Assert.assertFalse(context.isInitChannelPools());
    }
}
