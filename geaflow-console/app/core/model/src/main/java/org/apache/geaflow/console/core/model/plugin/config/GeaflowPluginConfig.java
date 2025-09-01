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

import com.google.common.base.Preconditions;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.geaflow.console.common.util.type.GeaflowPluginCategory;
import org.apache.geaflow.console.common.util.type.GeaflowPluginType;
import org.apache.geaflow.console.core.model.GeaflowName;
import org.apache.geaflow.console.core.model.config.ConfigDescFactory;
import org.apache.geaflow.console.core.model.config.GeaflowConfig;

@Getter
@Setter
@NoArgsConstructor
@Slf4j
public class GeaflowPluginConfig extends GeaflowName {

    private String type;

    private GeaflowPluginCategory category;

    private GeaflowConfig config;

    public GeaflowPluginConfig(GeaflowPluginCategory category, PluginConfigClass pluginConfigClass) {
        this.category = category;
        this.type = pluginConfigClass.getType().name();
        this.config = pluginConfigClass.build();
    }

    public GeaflowPluginConfig(String name, String comment, GeaflowPluginType type, GeaflowPluginCategory category,
                               GeaflowConfig config) {
        super.name = name;
        super.comment = comment;
        this.type = type.name();
        this.category = category;
        this.config = config;
    }

    @Override
    public void validate() {
        super.validate();
        Preconditions.checkNotNull(type, "Invalid plugin type");
        Preconditions.checkNotNull(category, "Invalid category");
        Preconditions.checkNotNull(config, "Invalid plugin config");

        GeaflowPluginType geaflowPluginType = GeaflowPluginType.of(type);
        if (geaflowPluginType != GeaflowPluginType.None) {
            ConfigDescFactory.get(geaflowPluginType).validateConfig(config);
        }

    }
}
