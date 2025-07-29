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

package org.apache.geaflow.dsl.connector.kafka;

import org.apache.geaflow.common.config.ConfigKey;
import org.apache.geaflow.common.config.ConfigKeys;

public class KafkaConfigKeys {

    public static final ConfigKey GEAFLOW_DSL_KAFKA_SERVERS = ConfigKeys
        .key("geaflow.dsl.kafka.servers")
        .noDefaultValue()
        .description("The kafka bootstrap servers list.");

    public static final ConfigKey GEAFLOW_DSL_KAFKA_TOPIC = ConfigKeys
        .key("geaflow.dsl.kafka.topic")
        .noDefaultValue()
        .description("The kafka topic.");

    public static final ConfigKey GEAFLOW_DSL_KAFKA_GROUP_ID = ConfigKeys
        .key("geaflow.dsl.kafka.group.id")
        .defaultValue("default-group-id")
        .description("The kafka group id, default is 'default-group-id'.");

    public static final ConfigKey GEAFLOW_DSL_KAFKA_PULL_BATCH_SIZE = ConfigKeys
        .key("geaflow.dsl.kafka.pull.batch.size")
        .defaultValue(100)
        .description("The kafka pull batch size");

    public static final ConfigKey GEAFLOW_DSL_KAFKA_DATA_OPERATION_TIMEOUT = ConfigKeys
        .key("geaflow.dsl.kafka.data.operation.timeout.seconds")
        .defaultValue(30)
        .description("The kafka pool/write data timeout");

    public static final ConfigKey GEAFLOW_DSL_KAFKA_CLIENT_ID = ConfigKeys
        .key("geaflow.dsl.kafka.client.id")
        .defaultValue(null)
        .description("The kafka client id");
}
