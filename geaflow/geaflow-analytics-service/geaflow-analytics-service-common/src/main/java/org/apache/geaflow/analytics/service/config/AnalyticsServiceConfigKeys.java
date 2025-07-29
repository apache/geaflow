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

public class AnalyticsServiceConfigKeys implements Serializable {

    public static final ConfigKey MAX_REQUEST_PER_SERVER = ConfigKeys
        .key("geaflow.max.request.per.server")
        .defaultValue(1)
        .description("the maximum number of requests that can be accepted simultaneously for per server");

    public static final ConfigKey ANALYTICS_SERVICE_PORT = ConfigKeys
        .key("geaflow.analytics.service.port")
        .defaultValue(0)
        .description("analytics service port, default is 0");

    public static final ConfigKey ANALYTICS_QUERY_PARALLELISM = ConfigKeys
        .key("geaflow.analytics.query.parallelism")
        .defaultValue(1)
        .description("analytics query parallelism");

    public static final ConfigKey ANALYTICS_QUERY = ConfigKeys
        .key("geaflow.analytics.query")
        .noDefaultValue()
        .description("analytics query");

    public static final ConfigKey ANALYTICS_SERVICE_REGISTER_ENABLE = ConfigKeys
        .key("geaflow.analytics.service.register.enable")
        .defaultValue(true)
        .description("enable analytics service info register");

    public static final ConfigKey ANALYTICS_COMPILE_SCHEMA_ENABLE = ConfigKeys
        .key("geaflow.analytics.compile.schema.enable")
        .defaultValue(true)
        .description("enable analytics compile schema");
}
