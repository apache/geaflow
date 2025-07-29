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

package org.apache.geaflow.dsl.connector.random;

import org.apache.geaflow.common.config.ConfigKey;
import org.apache.geaflow.common.config.ConfigKeys;

public class RandomSourceConfigKeys {

    public static final ConfigKey GEAFLOW_DSL_RANDOM_SOURCE_RATE = ConfigKeys
        .key("geaflow.dsl.random.source.tuples.per.second")
        .defaultValue(1.0)
        .description("Random source create tuple number per second");

    public static final ConfigKey GEAFLOW_DSL_RANDOM_SOURCE_MAX_BATCH = ConfigKeys
        .key("geaflow.dsl.random.source.max.batch")
        .defaultValue(3L)
        .description("Random source create batch max num");
}
