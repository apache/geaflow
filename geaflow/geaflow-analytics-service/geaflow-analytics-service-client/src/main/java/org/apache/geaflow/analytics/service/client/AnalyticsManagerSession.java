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

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

import com.google.common.collect.ImmutableMap;
import java.net.URI;
import java.util.Map;

public class AnalyticsManagerSession {

    private static final String USER = "user";
    private static final String SERVER = "server";
    private static final String PROPERTIES = "properties";
    private URI server;
    private String user;
    private String source;
    private Map<String, String> properties;
    private Map<String, String> customHeaders;
    private boolean compressionDisabled;
    private long clientRequestTimeoutMs;

    public static Builder builder() {
        return new Builder();
    }

    public AnalyticsManagerSession(URI server, String user, String source, long clientRequestTimeoutMs,
                                   boolean compressionDisabled, Map<String, String> properties,
                                   Map<String, String> customHeaders) {
        this.server = requireNonNull(server, "server is null");
        this.user = user;
        this.source = source;
        this.compressionDisabled = compressionDisabled;
        this.clientRequestTimeoutMs = clientRequestTimeoutMs;
        this.properties = ImmutableMap.copyOf(requireNonNull(properties, "properties is null"));
        this.customHeaders = ImmutableMap.copyOf(
            requireNonNull(customHeaders, "customHeaders is null"));
    }

    public AnalyticsManagerSession() {
    }

    public long getClientRequestTimeout() {
        return clientRequestTimeoutMs;
    }

    public URI getServer() {
        return server;
    }

    public String getUser() {
        return user;
    }

    public String getSource() {
        return source;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public Map<String, String> getCustomHeaders() {
        return customHeaders;
    }

    public boolean isCompressionDisabled() {
        return compressionDisabled;
    }

    @Override
    public String toString() {
        return toStringHelper(this)
            .add(SERVER, server)
            .add(USER, user)
            .add(PROPERTIES, properties)
            .omitNullValues()
            .toString();
    }

    public static final class Builder {

        private URI server;
        private String user;
        private String source;
        private Map<String, String> properties;
        private Map<String, String> customHeaders;
        private boolean compressionDisabled;
        private long clientRequestTimeoutMs;

        public Builder setServer(URI server) {
            this.server = server;
            return this;
        }

        public Builder setClientUser(String user) {
            this.user = user;
            return this;
        }

        public Builder setClientSource(String source) {
            this.source = source;
            return this;
        }

        public Builder setCompressionDisabled(boolean compressionDisabled) {
            this.compressionDisabled = compressionDisabled;
            return this;
        }

        public Builder setClientRequestTimeoutMs(long clientRequestTimeoutMs) {
            this.clientRequestTimeoutMs = clientRequestTimeoutMs;
            return this;
        }

        public Builder setCustomHeaders(Map<String, String> customHeaders) {
            this.customHeaders = customHeaders;
            return this;
        }

        public Builder setProperties(Map<String, String> properties) {
            this.properties = properties;
            return this;
        }

        public AnalyticsManagerSession build() {
            return new AnalyticsManagerSession(this);
        }
    }

    private AnalyticsManagerSession(Builder builder) {
        this.server = builder.server;
        this.user = builder.user;
        this.source = builder.source;
        this.compressionDisabled = builder.compressionDisabled;
        this.properties = builder.properties;
        this.customHeaders = builder.customHeaders;
        this.clientRequestTimeoutMs = builder.clientRequestTimeoutMs;
    }
}
