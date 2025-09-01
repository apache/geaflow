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

package org.apache.geaflow.dsl.connector.pulsar;

import org.apache.geaflow.common.config.ConfigKey;
import org.apache.geaflow.common.config.ConfigKeys;

public class PulsarConfigKeys {

    public static final ConfigKey GEAFLOW_DSL_PULSAR_SERVERS = ConfigKeys
        .key("geaflow.dsl.pulsar.servers")
        .noDefaultValue()
        .description("The pulsar bootstrap servers list.");

    public static final ConfigKey GEAFLOW_DSL_PULSAR_PORT = ConfigKeys
        .key("geaflow.dsl.pulsar.port")
        .noDefaultValue()
        .description("The pulsar bootstrap servers list.");

    public static final ConfigKey GEAFLOW_DSL_PULSAR_TOPIC = ConfigKeys
        .key("geaflow.dsl.pulsar.topic")
        .noDefaultValue()
        .description("The pulsar topic.");

    public static final ConfigKey GEAFLOW_DSL_PULSAR_SUBSCRIBE_NAME = ConfigKeys
        .key("geaflow.dsl.pulsar.subscribeName")
        .defaultValue("default-subscribeName")
        .description("The pulsar subscribeName, default is 'default-subscribeName'.");

    public static final ConfigKey GEAFLOW_DSL_PULSAR_SUBSCRIBE_INITIAL_POSITION = ConfigKeys
        .key("geaflow.dsl.pulsar.subscriptionInitialPosition")
        .defaultValue("latest")
        .description("The pulsar subscriptionInitialPosition, default is 'default-subscriptionInitialPosition'.");


}
