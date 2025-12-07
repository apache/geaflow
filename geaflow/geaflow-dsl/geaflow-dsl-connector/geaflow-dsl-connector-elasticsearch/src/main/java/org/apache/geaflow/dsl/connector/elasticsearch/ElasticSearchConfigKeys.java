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
        .description("ElasticSearch cluster hosts list (comma-separated).");

    public static final ConfigKey GEAFLOW_DSL_ELASTICSEARCH_INDEX = ConfigKeys
        .key("geaflow.dsl.elasticsearch.index")
        .noDefaultValue()
        .description("ElasticSearch index name.");

    public static final ConfigKey GEAFLOW_DSL_ELASTICSEARCH_TIMESTAMP_FIELD = ConfigKeys
        .key("geaflow.dsl.elasticsearch.timestamp.field")
        .defaultValue("@timestamp")
        .description("ElasticSearch timestamp field name.");

    public static final ConfigKey GEAFLOW_DSL_ELASTICSEARCH_MAX_FETCH_SIZE = ConfigKeys
        .key("geaflow.dsl.elasticsearch.max.fetch.size")
        .defaultValue(1000)
        .description("Batch size for each fetch operation.");


    public static final ConfigKey GEAFLOW_DSL_ELASTICSEARCH_CONNECT_TIMEOUT = ConfigKeys
            .key("geaflow.dsl.elasticsearch.connect.timeout.ms")
            .defaultValue(5000)
            .description("ElasticSearch client connection timeout in milliseconds.");

    public static final ConfigKey GEAFLOW_DSL_ELASTICSEARCH_SOCKET_TIMEOUT = ConfigKeys
            .key("geaflow.dsl.elasticsearch.socket.timeout.ms")
            .defaultValue(60000)
            .description("ElasticSearch client socket timeout in milliseconds.");

    public static final ConfigKey GEAFLOW_DSL_ELASTICSEARCH_REQUEST_TIMEOUT = ConfigKeys
            .key("geaflow.dsl.elasticsearch.request.timeout.ms")
            .defaultValue(1000)
            .description("ElasticSearch client request timeout in milliseconds.");

    public static final ConfigKey GEAFLOW_DSL_ELASTICSEARCH_ID_FIELD = ConfigKeys
            .key("geaflow.dsl.elasticsearch.id.field")
            .noDefaultValue()
            .description("ElasticSearch client field name.");

    public static final ConfigKey GEAFLOW_DSL_ELASTICSEARCH_FLUSH_INTERVAL_MS = ConfigKeys
            .key("geaflow.dsl.elasticsearch.flush.interval.ms")
            .defaultValue(1000L)
            .description("ElasticSearch client flush interval in milliseconds.");

    public static final ConfigKey GEAFLOW_DSL_ELASTICSEARCH_BULK_SIZE_MB = ConfigKeys
            .key("geaflow.dsl.elasticsearch.bulk.size.mb")
            .defaultValue(5L)
            .description("ElasticSearch client bulk size in megabytes.");

    public static final ConfigKey GEAFLOW_DSL_ELASTICSEARCH_CONCURRENT_REQUESTS = ConfigKeys
            .key("geaflow.dsl.elasticsearch.concurrent.requests")
            .defaultValue(1)
            .description("ElasticSearch client concurrent requests");

    public static final ConfigKey GEAFLOW_DSL_ELASTICSEARCH_BACKOFF = ConfigKeys
            .key("geaflow.dsl.elasticsearch.backoff")
            .defaultValue("true")
            .description("ElasticSearch client backoff");

    public static final ConfigKey GEAFLOW_DSL_ELASTICSEARCH_USERNAME = ConfigKeys
            .key("geaflow.dsl.elasticsearch.username")
            .noDefaultValue()
            .description("Elasticsearch username for authentication.");

    public static final ConfigKey GEAFLOW_DSL_ELASTICSEARCH_PASSWORD = ConfigKeys
            .key("geaflow.dsl.elasticsearch.password")
            .noDefaultValue()
            .description("Elasticsearch password for authentication.");


}