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
import org.apache.geaflow.console.common.util.type.GeaflowPluginType;
import org.apache.geaflow.console.core.model.config.GeaflowConfigKey;
import org.apache.geaflow.console.core.model.config.GeaflowConfigValue;

@Getter
@Setter
public abstract class PersistentPluginConfigClass extends PluginConfigClass {

    @GeaflowConfigKey(value = "geaflow.file.persistent.root", comment = "i18n.key.root.path", jsonIgnore = true)
    @GeaflowConfigValue(required = true, defaultValue = "/")
    private String root;

    @GeaflowConfigKey(value = "geaflow.file.persistent.user.name", comment = "i18n.key.username", jsonIgnore = true)
    @GeaflowConfigValue
    private String username;

    @GeaflowConfigKey(value = "geaflow.file.persistent.thread.size", comment = "i18n.key.local.thread.pool.count", jsonIgnore = true)
    @GeaflowConfigValue
    private Integer threadSize;

    public PersistentPluginConfigClass(GeaflowPluginType type) {
        super(type);
    }
}
