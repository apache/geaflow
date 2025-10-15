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
import lombok.extern.slf4j.Slf4j;
import org.apache.geaflow.console.common.util.type.GeaflowPluginType;
import org.apache.geaflow.console.core.model.config.GeaflowConfigKey;
import org.apache.geaflow.console.core.model.config.GeaflowConfigValue;

@Slf4j
@Getter
@Setter
public class RayPluginConfigClass extends PluginConfigClass {

    public RayPluginConfigClass() {
        super(GeaflowPluginType.RAY);
    }

    @GeaflowConfigKey(value = "ray.dashboard.address", comment = "ray.dashboard.address")
    @GeaflowConfigValue(required = true, defaultValue = "http://127.0.0.1:8090")
    private String dashboardAddress;

    @GeaflowConfigKey(value = "ray.redis.address", comment = "ray.redis.address")
    @GeaflowConfigValue(required = true, defaultValue = "127.0.0.1:6379")
    private String redisAddress;


    @GeaflowConfigKey(value = "ray.dist.jar.path", comment = "ray.dist.jar.path")
    @GeaflowConfigValue(required = true)
    private String distJarPath;

    @GeaflowConfigKey(value = "ray.session.resource.jar.path", comment = "ray.session.resource.jar.path")
    @GeaflowConfigValue(required = true)
    private String sessionResourceJarPath;
}
