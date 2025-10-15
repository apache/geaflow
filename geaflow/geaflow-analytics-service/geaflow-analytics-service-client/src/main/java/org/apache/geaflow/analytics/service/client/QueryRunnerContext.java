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

public class QueryRunnerContext {
    private final Configuration configuration;
    private final boolean initChannelPools;
    private final HostAndPort hostAndPort;
    private final String metaServerBaseNode;
    private final String metaServerAddress;

    public Configuration getConfiguration() {
        return configuration;
    }

    public HostAndPort getHostAndPort() {
        return hostAndPort;
    }

    public boolean isInitChannelPools() {
        return initChannelPools;
    }

    public String getMetaServerBaseNode() {
        return metaServerBaseNode;
    }

    public String getMetaServerAddress() {
        return metaServerAddress;
    }

    public boolean enableInitChannelPools() {
        return initChannelPools;
    }

    public static ClientHandlerContextBuilder newBuilder() {
        return new ClientHandlerContextBuilder();
    }

    public static class ClientHandlerContextBuilder {
        private Configuration configuration;
        private boolean initChannelPools;

        private HostAndPort hostAndPort;
        private String analyticsServiceJobName;
        private String metaServerAddress;

        public ClientHandlerContextBuilder setAnalyticsServiceJobName(String analyticsServiceJobName) {
            this.analyticsServiceJobName = analyticsServiceJobName;
            return this;
        }

        public ClientHandlerContextBuilder setMetaServerAddress(String metaServerAddress) {
            this.metaServerAddress = metaServerAddress;
            return this;
        }

        public ClientHandlerContextBuilder setConfiguration(Configuration configuration) {
            this.configuration = configuration;
            return this;
        }

        public ClientHandlerContextBuilder setHost(HostAndPort hostAndPort) {
            this.hostAndPort = hostAndPort;
            return this;
        }

        public ClientHandlerContextBuilder enableInitChannelPools(boolean initChannelPools) {
            this.initChannelPools = initChannelPools;
            return this;
        }

        public QueryRunnerContext build() {
            return new QueryRunnerContext(this);
        }
    }


    private QueryRunnerContext(ClientHandlerContextBuilder builder) {
        this.configuration = builder.configuration;
        this.initChannelPools = builder.initChannelPools;
        this.hostAndPort = builder.hostAndPort;
        this.metaServerAddress = builder.metaServerAddress;
        this.metaServerBaseNode = builder.analyticsServiceJobName;
    }
}
