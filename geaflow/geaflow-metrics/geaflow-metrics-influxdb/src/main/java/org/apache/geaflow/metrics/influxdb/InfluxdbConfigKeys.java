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

import org.apache.geaflow.common.config.ConfigKey;
import org.apache.geaflow.common.config.ConfigKeys;

public class InfluxdbConfigKeys {

    public static final ConfigKey URL = ConfigKeys
        .key("geaflow.metric.influxdb.url")
        .noDefaultValue()
        .description("influxdb url, e.g. http://localhost:8086");

    public static final ConfigKey TOKEN = ConfigKeys
        .key("geaflow.metric.influxdb.token")
        .noDefaultValue()
        .description("influxdb token, for authorization");

    public static final ConfigKey ORG = ConfigKeys
        .key("geaflow.metric.influxdb.org")
        .noDefaultValue()
        .description("influxdb organization of the bucket");

    public static final ConfigKey BUCKET = ConfigKeys
        .key("geaflow.metric.influxdb.bucket")
        .noDefaultValue()
        .description("influxdb bucket name");

    public static final ConfigKey CONNECT_TIMEOUT_MS = ConfigKeys
        .key("geaflow.metric.influxdb.connect.timeout.ms")
        .defaultValue(30_000L)
        .description("influxdb connect timeout millis");

    public static final ConfigKey WRITE_TIMEOUT_MS = ConfigKeys
        .key("geaflow.metric.influxdb.write.timeout.ms")
        .defaultValue(30_000L)
        .description("influxdb write timeout millis");

}
