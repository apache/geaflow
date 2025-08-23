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
public class FilePluginConfigClass extends PluginConfigClass {

    @GeaflowConfigKey(value = "geaflow.dsl.file.path", comment = "i18n.key.file.path")
    @GeaflowConfigValue(required = true, defaultValue = "/")
    private String filePath;

    @GeaflowConfigKey(value = "geaflow.dsl.column.separator", comment = "i18n.key.column.separator")
    @GeaflowConfigValue(defaultValue = ",")
    private String columnSeparator;

    @GeaflowConfigKey(value = "geaflow.dsl.line.separator", comment = "i18n.key.line.separator")
    @GeaflowConfigValue(defaultValue = "\\n")
    private String lineSeparator;

    @GeaflowConfigKey(value = "geaflow.file.persistent.config.json", comment = "i18n.key.ext.config.json")
    @GeaflowConfigValue(defaultValue = "{\"fs.defaultFS\":\"local\"}")
    private String persistConfig;

    public FilePluginConfigClass() {
        super(GeaflowPluginType.FILE);
    }
}
