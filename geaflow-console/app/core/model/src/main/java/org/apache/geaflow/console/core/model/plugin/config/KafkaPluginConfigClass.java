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

package org.apache.geaflow.console.core.model.plugin.config;

import lombok.Getter;
import lombok.Setter;
import org.apache.geaflow.console.common.util.NetworkUtil;
import org.apache.geaflow.console.common.util.type.GeaflowPluginType;
import org.apache.geaflow.console.core.model.config.GeaflowConfigKey;
import org.apache.geaflow.console.core.model.config.GeaflowConfigValue;

@Getter
@Setter
public class KafkaPluginConfigClass extends PluginConfigClass {

    @GeaflowConfigKey(value = "geaflow.dsl.kafka.servers", comment = "i18n.key.servers")
    @GeaflowConfigValue(required = true, defaultValue = "0.0.0.0:9092")
    private String servers;

    @GeaflowConfigKey(value = "geaflow.dsl.kafka.group.id", comment = "i18n.key.group.id")
    @GeaflowConfigValue(required = true)
    private String group;

    @GeaflowConfigKey(value = "geaflow.dsl.kafka.topic", comment = "i18n.key.topic")
    @GeaflowConfigValue(required = true)
    private String topic;

    public KafkaPluginConfigClass() {
        super(GeaflowPluginType.KAFKA);
    }

    @Override
    public void testConnection() {
        NetworkUtil.testUrls(servers, ",");
    }
}
