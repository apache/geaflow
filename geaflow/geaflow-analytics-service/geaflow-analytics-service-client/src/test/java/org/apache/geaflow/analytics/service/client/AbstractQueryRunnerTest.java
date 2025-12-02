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

import java.util.concurrent.atomic.AtomicReference;
import org.apache.geaflow.analytics.service.query.QueryResults;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.common.rpc.HostAndPort;
import org.apache.geaflow.pipeline.service.ServiceType;
import org.testng.Assert;
import org.testng.annotations.Test;

public class AbstractQueryRunnerTest {

    @Test
    public void testQueryRunnerStatusMethods() {
        try (TestQueryRunner runner = new TestQueryRunner()) {
            QueryRunnerContext context = QueryRunnerContext.newBuilder()
                .setConfiguration(new Configuration())
                .setHost(new HostAndPort("localhost", 8080))
                .build();
            
            runner.init(context);
            
            Assert.assertTrue(runner.isRunning());
            Assert.assertFalse(runner.isAborted());
            Assert.assertFalse(runner.isError());
            Assert.assertFalse(runner.isFinished());
            
            runner.setStatus(QueryRunnerStatus.ERROR);
            Assert.assertFalse(runner.isRunning());
            Assert.assertTrue(runner.isError());
            
            runner.setStatus(QueryRunnerStatus.ABORTED);
            Assert.assertTrue(runner.isAborted());
            
            runner.setStatus(QueryRunnerStatus.FINISHED);
            Assert.assertTrue(runner.isFinished());
        }
    }

    @Test
    public void testInitWithHostAndPort() {
        try (TestQueryRunner runner = new TestQueryRunner()) {
            Configuration config = new Configuration();
            HostAndPort hostAndPort = new HostAndPort("test-host", 9090);
            
            QueryRunnerContext context = QueryRunnerContext.newBuilder()
                .setConfiguration(config)
                .setHost(hostAndPort)
                .build();
            
            runner.init(context);
            
            Assert.assertNotNull(runner.getAnalyticsServiceInfo());
            Assert.assertEquals(runner.getAnalyticsServiceInfo().getCoordinatorNum(), 1);
            Assert.assertEquals(runner.getAnalyticsServiceInfo().getCoordinatorAddresses(0), hostAndPort);
        }
    }

    private static class TestQueryRunner extends AbstractQueryRunner {

        @Override
        protected void initManagedChannel(HostAndPort address) {
            // No-op for test
        }

        @Override
        public QueryResults executeQuery(String queryScript) {
            return null;
        }

        @Override
        public ServiceType getServiceType() {
            return ServiceType.analytics_http;
        }

        @Override
        public QueryResults cancelQuery(long queryId) {
            return null;
        }

        @Override
        public void close() {
            // No-op for test
        }

        public void setStatus(QueryRunnerStatus status) {
            if (this.queryRunnerStatus == null) {
                this.queryRunnerStatus = new AtomicReference<>(status);
            } else {
                this.queryRunnerStatus.set(status);
            }
        }

        public AnalyticsServiceInfo getAnalyticsServiceInfo() {
            return this.analyticsServiceInfo;
        }
    }
}
