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

public class ElasticsearchConfigKeys {

    public static final ConfigKey GEAFLOW_DSL_ELASTICSEARCH_HOSTS = ConfigKeys
            .key("geaflow.dsl.elasticsearch.hosts")
            .noDefaultValue()
            .description("Elasticsearch cluster hosts (comma-separated), e.g., 'localhost:9200,host2:9200'");

    public static final ConfigKey GEAFLOW_DSL_ELASTICSEARCH_USERNAME = ConfigKeys
            .key("geaflow.dsl.elasticsearch.username")
            .defaultValue("")
            .description("Username for Elasticsearch authentication");

    public static final ConfigKey GEAFLOW_DSL_ELASTICSEARCH_PASSWORD = ConfigKeys
            .key("geaflow.dsl.elasticsearch.password")
            .defaultValue("")
            .description("Password for Elasticsearch authentication");

    public static final ConfigKey GEAFLOW_DSL_ELASTICSEARCH_INDEX = ConfigKeys
            .key("geaflow.dsl.elasticsearch.index")
            .noDefaultValue()
            .description("Elasticsearch index name");

    public static final ConfigKey GEAFLOW_DSL_ELASTICSEARCH_ID_FIELD = ConfigKeys
            .key("geaflow.dsl.elasticsearch.id.field")
            .defaultValue(ElasticsearchConstants.DEFAULT_ID_FIELD)
            .description("Field name to use as document ID");

    public static final ConfigKey GEAFLOW_DSL_ELASTICSEARCH_BATCH_SIZE = ConfigKeys
            .key("geaflow.dsl.elasticsearch.batch.size")
            .defaultValue(String.valueOf(ElasticsearchConstants.DEFAULT_BATCH_SIZE))
            .description("Batch size for bulk operations");

    public static final ConfigKey GEAFLOW_DSL_ELASTICSEARCH_MAX_RETRIES = ConfigKeys
            .key("geaflow.dsl.elasticsearch.max.retries")
            .defaultValue(String.valueOf(ElasticsearchConstants.DEFAULT_MAX_RETRIES))
            .description("Maximum number of retries for failed requests");

    public static final ConfigKey GEAFLOW_DSL_ELASTICSEARCH_CONNECT_TIMEOUT = ConfigKeys
            .key("geaflow.dsl.elasticsearch.connect.timeout.ms")
            .defaultValue(String.valueOf(ElasticsearchConstants.DEFAULT_CONNECT_TIMEOUT_MILLIS))
            .description("Connection timeout in milliseconds");

    public static final ConfigKey GEAFLOW_DSL_ELASTICSEARCH_SOCKET_TIMEOUT = ConfigKeys
            .key("geaflow.dsl.elasticsearch.socket.timeout.ms")
            .defaultValue(String.valueOf(ElasticsearchConstants.DEFAULT_SOCKET_TIMEOUT_MILLIS))
            .description("Socket timeout in milliseconds");

    public static final ConfigKey GEAFLOW_DSL_ELASTICSEARCH_MAX_CONNECTIONS = ConfigKeys
            .key("geaflow.dsl.elasticsearch.max.connections")
            .defaultValue(String.valueOf(ElasticsearchConstants.DEFAULT_MAX_CONNECTIONS))
            .description("Maximum number of connections in the pool");

    public static final ConfigKey GEAFLOW_DSL_ELASTICSEARCH_SCROLL_TIMEOUT = ConfigKeys
            .key("geaflow.dsl.elasticsearch.scroll.timeout")
            .defaultValue(ElasticsearchConstants.DEFAULT_SCROLL_TIMEOUT)
            .description("Scroll context timeout, e.g., '5m', '1h'");

    public static final ConfigKey GEAFLOW_DSL_ELASTICSEARCH_SCROLL_SIZE = ConfigKeys
            .key("geaflow.dsl.elasticsearch.scroll.size")
            .defaultValue(String.valueOf(ElasticsearchConstants.DEFAULT_SCROLL_SIZE))
            .description("Number of documents per scroll request");

    public static final ConfigKey GEAFLOW_DSL_ELASTICSEARCH_QUERY = ConfigKeys
            .key("geaflow.dsl.elasticsearch.query")
            .defaultValue("{\"match_all\":{}}")
            .description("Elasticsearch query DSL in JSON format");
}
