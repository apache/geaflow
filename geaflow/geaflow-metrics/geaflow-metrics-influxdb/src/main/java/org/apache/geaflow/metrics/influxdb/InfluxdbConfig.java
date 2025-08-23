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

package org.apache.geaflow.metrics.influxdb;

import org.apache.geaflow.common.config.Configuration;

public class InfluxdbConfig {

    private final String url;
    private final String token;
    private final String org;
    private final String bucket;
    private final long connectTimeoutMs;
    private final long writeTimeoutMs;

    public InfluxdbConfig(Configuration config) {
        this.url = config.getString(InfluxdbConfigKeys.URL);
        this.token = config.getString(InfluxdbConfigKeys.TOKEN);
        this.org = config.getString(InfluxdbConfigKeys.ORG);
        this.bucket = config.getString(InfluxdbConfigKeys.BUCKET);
        this.connectTimeoutMs = config.getLong(InfluxdbConfigKeys.CONNECT_TIMEOUT_MS);
        this.writeTimeoutMs = config.getLong(InfluxdbConfigKeys.WRITE_TIMEOUT_MS);
    }

    public String getUrl() {
        return this.url;
    }

    public String getToken() {
        return this.token;
    }

    public String getOrg() {
        return this.org;
    }

    public String getBucket() {
        return this.bucket;
    }

    public long getConnectTimeoutMs() {
        return this.connectTimeoutMs;
    }

    public long getWriteTimeoutMs() {
        return this.writeTimeoutMs;
    }

    @Override
    public String toString() {
        return "InfluxdbConfig{"
            + "url='" + url + '\''
            + ", token='" + token + '\''
            + ", org='" + org + '\''
            + ", bucket='" + bucket + '\''
            + ", connectTimeoutMs=" + connectTimeoutMs
            + ", writeTimeoutMs=" + writeTimeoutMs
            + '}';
    }

}
