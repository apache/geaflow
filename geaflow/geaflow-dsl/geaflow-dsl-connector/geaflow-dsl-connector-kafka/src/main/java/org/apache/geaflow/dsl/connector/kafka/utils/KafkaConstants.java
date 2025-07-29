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

package org.apache.geaflow.dsl.connector.kafka.utils;

public class KafkaConstants {

    public static final String KAFKA_BOOTSTRAP_SERVERS = "bootstrap.servers";
    public static final String KAFKA_KEY_SERIALIZER = "key.serializer";
    public static final String KAFKA_VALUE_SERIALIZER = "value.serializer";
    public static final String KAFKA_KEY_DESERIALIZER = "key.deserializer";
    public static final String KAFKA_VALUE_DESERIALIZER = "value.deserializer";
    public static final String KAFKA_MAX_POLL_RECORDS = "max.poll.records";
    public static final String KAFKA_GROUP_ID = "group.id";
    public static final String KAFKA_CLIENT_ID = "client.id";
    public static final String KAFKA_VALUE_SERIALIZER_CLASS =
        "org.apache.kafka.common.serialization.StringSerializer";
    public static final String KAFKA_VALUE_DESERIALIZER_CLASS =
        "org.apache.kafka.common.serialization.StringDeserializer";

    public static final String KAFKA_BEGIN = "begin";
    public static final String KAFKA_LATEST = "latest";
    public static final int KAFKA_OPERATION_TIMEOUT_SECONDS = 10;
}
