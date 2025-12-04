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

package org.apache.geaflow.dsl.connector.elasticsearch;

import org.apache.geaflow.common.config.ConfigKey;
import org.apache.geaflow.common.config.ConfigKeys;

public class ElasticSearchConfigKeys {

    public static final ConfigKey GEAFLOW_DSL_ELASTICSEARCH_HOSTS = ConfigKeys
        .key("geaflow.dsl.elasticsearch.hosts")
        .noDefaultValue()
        .description("ElasticSearch hosts, separated by comma.");

    public static final ConfigKey GEAFLOW_DSL_ELASTICSEARCH_INDEX = ConfigKeys
        .key("geaflow.dsl.elasticsearch.index")
        .noDefaultValue()
        .description("ElasticSearch index name.");

    public static final ConfigKey GEAFLOW_DSL_ELASTICSEARCH_USERNAME = ConfigKeys
        .key("geaflow.dsl.elasticsearch.username")
        .noDefaultValue()
        .description("ElasticSearch username.");

    public static final ConfigKey GEAFLOW_DSL_ELASTICSEARCH_PASSWORD = ConfigKeys
        .key("geaflow.dsl.elasticsearch.password")
        .noDefaultValue()
        .description("ElasticSearch password.");

    public static final ConfigKey GEAFLOW_DSL_ELASTICSEARCH_FLUSH_SIZE = ConfigKeys
        .key("geaflow.dsl.elasticsearch.flush.size")
        .defaultValue(100)
        .description("Number of records to flush at once.");

    public static final ConfigKey GEAFLOW_DSL_ELASTICSEARCH_RETRY_TIMES = ConfigKeys
        .key("geaflow.dsl.elasticsearch.retry.times")
        .defaultValue(3)
        .description("Number of retry times when write failed.");
}
