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
import lombok.Setter;
import org.apache.geaflow.console.core.model.config.GeaflowConfigClass;
import org.apache.geaflow.console.core.model.config.GeaflowConfigKey;
import org.apache.geaflow.console.core.model.config.GeaflowConfigValue;

@Getter
@Setter
public class ClusterConfigClass extends GeaflowConfigClass {

    @GeaflowConfigKey(value = "geaflow.container.num", comment = "i18n.key.container.count")
    @GeaflowConfigValue(required = true)
    private Integer containers;

    @GeaflowConfigKey(value = "geaflow.container.worker.num", comment = "i18n.key.container.worker.count")
    @GeaflowConfigValue(required = true)
    private Integer containerWorkers;

    @GeaflowConfigKey(value = "geaflow.container.memory.mb", comment = "i18n.key.container.memory.mb")
    @GeaflowConfigValue(required = true, defaultValue = "256")
    private Integer containerMemory;

    @GeaflowConfigKey(value = "geaflow.container.vcores", comment = "i18n.key.container.vcores")
    @GeaflowConfigValue(required = true, defaultValue = "1")
    private Double containerCores;

    @GeaflowConfigKey(value = "geaflow.container.jvm.options", comment = "i18n.key.container.jvm.args")
    @GeaflowConfigValue(required = true)
    private String containerJvmOptions;

    @GeaflowConfigKey(value = "geaflow.fo.enable", comment = "i18n.key.fo.enable")
    @GeaflowConfigValue(defaultValue = "true")
    private Boolean enableFo;

    @GeaflowConfigKey(value = "geaflow.client.memory.mb", comment = "i18n.key.client.memory.mb")
    @GeaflowConfigValue(defaultValue = "1024")
    private Integer clientMemory;

    @GeaflowConfigKey(value = "geaflow.master.memory.mb", comment = "i18n.key.master.memory.mb")
    @GeaflowConfigValue(defaultValue = "4096")
    private Integer masterMemory;

    @GeaflowConfigKey(value = "geaflow.driver.memory.mb", comment = "i18n.key.driver.memory.mb")
    @GeaflowConfigValue(defaultValue = "4096")
    private Integer driverMemory;

    @GeaflowConfigKey(value = "geaflow.client.vcores", comment = "i18n.key.client.vcores")
    @GeaflowConfigValue(defaultValue = "1")
    private Double clientCores;

    @GeaflowConfigKey(value = "geaflow.master.vcores", comment = "i18n.key.master.vcores")
    @GeaflowConfigValue(defaultValue = "1")
    private Double masterCores;

    @GeaflowConfigKey(value = "geaflow.driver.vcores", comment = "i18n.key.driver.vcores")
    @GeaflowConfigValue(defaultValue = "1")
    private Double driverCores;

    @GeaflowConfigKey(value = "geaflow.client.jvm.options", comment = "i18n.key.client.jvm.args")
    @GeaflowConfigValue(defaultValue = "-Xmx1024m,-Xms1024m,-Xmn256m,-Xss256k,-XX:MaxDirectMemorySize=512m")
    private String clientJvmOptions;

    @GeaflowConfigKey(value = "geaflow.master.jvm.options", comment = "i18n.key.master.jvm.args")
    @GeaflowConfigValue(defaultValue = "-Xmx2048m,-Xms2048m,-Xmn512m,-Xss512k,-XX:MaxDirectMemorySize=1024m")
    private String masterJvmOptions;

    @GeaflowConfigKey(value = "geaflow.driver.jvm.options", comment = "i18n.key.driver.jvm.args")
    @GeaflowConfigValue(defaultValue = "-Xmx2048m,-Xms2048m,-Xmn512m,-Xss512k,-XX:MaxDirectMemorySize=1024m")
    private String driverJvmOptions;

}
