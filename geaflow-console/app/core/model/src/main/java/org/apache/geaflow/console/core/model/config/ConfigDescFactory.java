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

package org.apache.geaflow.console.core.model.config;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import lombok.extern.slf4j.Slf4j;
import org.apache.geaflow.console.common.util.Fmt;
import org.apache.geaflow.console.common.util.exception.GeaflowException;
import org.apache.geaflow.console.common.util.exception.GeaflowLogException;
import org.apache.geaflow.console.common.util.type.GeaflowPluginType;
import org.apache.geaflow.console.core.model.plugin.config.PluginConfigClass;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class ConfigDescFactory {

    private static final Map<Class<? extends GeaflowConfigClass>, GeaflowConfigDesc> CONFIG_DESCS =
        new ConcurrentHashMap<>();

    private static final Map<GeaflowPluginType, GeaflowConfigDesc> PLUGIN_CONFIG_DESCS = new ConcurrentHashMap<>();

    static {
        String packageName = PluginConfigClass.class.getPackage().getName();
        for (GeaflowPluginType type : GeaflowPluginType.values()) {
            if (type == GeaflowPluginType.None) {
                continue;
            }
            String prefix = type.name().charAt(0) + type.name().substring(1).toLowerCase();
            String className = Fmt.as("{}.{}PluginConfigClass", packageName, prefix);

            try {
                Class<?> clazz = Class.forName(className);
                PLUGIN_CONFIG_DESCS.put(type, getOrRegister((Class<? extends GeaflowConfigClass>) clazz));
                log.info("Register {} plugin config class {} success", type, clazz.getSimpleName());

            } catch (Exception e) {
                throw new GeaflowLogException("Register {} plugin config failed", type, e);
            }
        }
    }

    public static GeaflowConfigDesc getOrRegister(Class<? extends GeaflowConfigClass> clazz) {
        if (!CONFIG_DESCS.containsKey(clazz)) {
            CONFIG_DESCS.put(clazz, new GeaflowConfigDesc(clazz));
        }

        return CONFIG_DESCS.get(clazz);
    }

    public static GeaflowConfigDesc get(GeaflowPluginType type) {
        GeaflowConfigDesc config = PLUGIN_CONFIG_DESCS.get(type);
        if (config == null) {
            throw new GeaflowException("Plugin config type {} not register", type);
        }
        return config;
    }
}
