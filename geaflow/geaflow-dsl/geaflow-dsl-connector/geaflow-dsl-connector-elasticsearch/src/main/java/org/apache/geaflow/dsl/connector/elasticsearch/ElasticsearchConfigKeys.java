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
            .description("Elasticsearch cluster hosts list.");

    public static final ConfigKey GEAFLOW_DSL_ELASTICSEARCH_INDEX = ConfigKeys
            .key("geaflow.dsl.elasticsearch.index")
            .noDefaultValue()
            .description("Elasticsearch index name.");

    public static final ConfigKey GEAFLOW_DSL_ELASTICSEARCH_DOCUMENT_ID_FIELD = ConfigKeys
            .key("geaflow.dsl.elasticsearch.document.id.field")
            .noDefaultValue()
            .description("Elasticsearch document id field.");

    public static final ConfigKey GEAFLOW_DSL_ELASTICSEARCH_USERNAME = ConfigKeys
            .key("geaflow.dsl.elasticsearch.username")
            .noDefaultValue()
            .description("Elasticsearch username for authentication.");

    public static final ConfigKey GEAFLOW_DSL_ELASTICSEARCH_PASSWORD = ConfigKeys
            .key("geaflow.dsl.elasticsearch.password")
            .noDefaultValue()
            .description("Elasticsearch password for authentication.");

    public static final ConfigKey GEAFLOW_DSL_ELASTICSEARCH_BATCH_SIZE = ConfigKeys
            .key("geaflow.dsl.elasticsearch.batch.size")
            .defaultValue("1000")
            .description("Elasticsearch batch write size.");

    public static final ConfigKey GEAFLOW_DSL_ELASTICSEARCH_SCROLL_TIMEOUT = ConfigKeys
            .key("geaflow.dsl.elasticsearch.scroll.timeout")
            .defaultValue("60s")
            .description("Elasticsearch scroll query timeout.");

    public static final ConfigKey GEAFLOW_DSL_ELASTICSEARCH_CONNECTION_TIMEOUT = ConfigKeys
            .key("geaflow.dsl.elasticsearch.connection.timeout")
            .defaultValue("1000")
            .description("Elasticsearch connection timeout in milliseconds.");

    public static final ConfigKey GEAFLOW_DSL_ELASTICSEARCH_SOCKET_TIMEOUT = ConfigKeys
            .key("geaflow.dsl.elasticsearch.socket.timeout")
            .defaultValue("30000")
            .description("Elasticsearch socket timeout in milliseconds.");
}
