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

package org.apache.geaflow.console.core.model.job.config;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.apache.geaflow.console.common.util.exception.GeaflowIllegalException;
import org.apache.geaflow.console.common.util.type.GeaflowPluginType;
import org.apache.geaflow.console.core.model.config.ConfigValueBehavior;
import org.apache.geaflow.console.core.model.config.GeaflowConfigClass;
import org.apache.geaflow.console.core.model.config.GeaflowConfigKey;
import org.apache.geaflow.console.core.model.config.GeaflowConfigValue;
import org.apache.geaflow.console.core.model.plugin.config.GeaflowPluginConfig;
import org.apache.geaflow.console.core.model.plugin.config.InfluxdbPluginConfigClass;
import org.apache.geaflow.console.core.model.plugin.config.PluginConfigClass;

@Getter
@Setter
@NoArgsConstructor
public class MetricArgsClass extends GeaflowConfigClass {

    @GeaflowConfigKey(value = "geaflow.metric.reporters", comment = "i18n.key.metric.storage.type")
    @GeaflowConfigValue(required = true, defaultValue = "INFLUXDB")
    private GeaflowPluginType type;

    @GeaflowConfigKey(value = "plugin", comment = "i18n.key.plugin.config")
    @GeaflowConfigValue(required = true, behavior = ConfigValueBehavior.FLATTED)
    private PluginConfigClass plugin;

    public MetricArgsClass(GeaflowPluginConfig pluginConfig) {
        this.type = GeaflowPluginType.of(pluginConfig.getType());

        Class<? extends PluginConfigClass> configClass;
        switch (type) {
            case INFLUXDB:
                configClass = InfluxdbPluginConfigClass.class;
                break;
            default:
                throw new GeaflowIllegalException("Metric meta config type {} not supported", pluginConfig.getType());
        }

        this.plugin = pluginConfig.getConfig().parse(configClass);
    }
}
