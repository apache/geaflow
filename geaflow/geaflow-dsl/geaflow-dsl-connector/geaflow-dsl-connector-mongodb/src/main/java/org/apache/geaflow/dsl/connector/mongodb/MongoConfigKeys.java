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

package org.apache.geaflow.dsl.connector.mongodb;

import org.apache.geaflow.common.config.ConfigKey;
import org.apache.geaflow.common.config.ConfigKeys;

public class MongoConfigKeys {

    public static final ConfigKey GEAFLOW_DSL_MONGODB_URI = ConfigKeys
        .key("geaflow.dsl.mongodb.uri")
        .noDefaultValue()
        .description("MongoDB connection string.");

    public static final ConfigKey GEAFLOW_DSL_MONGODB_DATABASE = ConfigKeys
        .key("geaflow.dsl.mongodb.database")
        .noDefaultValue()
        .description("MongoDB database name.");

    public static final ConfigKey GEAFLOW_DSL_MONGODB_COLLECTION = ConfigKeys
        .key("geaflow.dsl.mongodb.collection")
        .noDefaultValue()
        .description("MongoDB collection name.");

    public static final ConfigKey GEAFLOW_DSL_MONGODB_BATCH_SIZE = ConfigKeys
        .key("geaflow.dsl.mongodb.batch.size")
        .defaultValue(1000)
        .description("MongoDB sink batch size.");

    public static final ConfigKey GEAFLOW_DSL_MONGODB_PARTITION_NUM = ConfigKeys
        .key("geaflow.dsl.mongodb.partition.num")
        .defaultValue(1)
        .description("MongoDB source partition number.");

    public static final ConfigKey GEAFLOW_DSL_MONGODB_PARTITION_FIELD = ConfigKeys
        .key("geaflow.dsl.mongodb.partition.field")
        .noDefaultValue()
        .description("MongoDB source range partition field.");

    public static final ConfigKey GEAFLOW_DSL_MONGODB_PARTITION_LOWERBOUND = ConfigKeys
        .key("geaflow.dsl.mongodb.partition.lowerbound")
        .noDefaultValue()
        .description("Inclusive lower bound for MongoDB source partitions.");

    public static final ConfigKey GEAFLOW_DSL_MONGODB_PARTITION_UPPERBOUND = ConfigKeys
        .key("geaflow.dsl.mongodb.partition.upperbound")
        .noDefaultValue()
        .description("Exclusive upper bound for MongoDB source partitions.");

    private MongoConfigKeys() {
    }
}
