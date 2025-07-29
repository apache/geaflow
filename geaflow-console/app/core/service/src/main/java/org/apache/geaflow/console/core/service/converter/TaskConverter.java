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

package org.apache.geaflow.console.core.service.converter;

import com.alibaba.fastjson.JSON;
import java.util.Optional;
import org.apache.geaflow.console.common.dal.entity.TaskEntity;
import org.apache.geaflow.console.core.model.plugin.config.GeaflowPluginConfig;
import org.apache.geaflow.console.core.model.release.GeaflowRelease;
import org.apache.geaflow.console.core.model.task.GeaflowTask;
import org.apache.geaflow.console.core.model.task.GeaflowTaskHandle;
import org.springframework.stereotype.Component;

@Component
public class TaskConverter extends IdConverter<GeaflowTask, TaskEntity> {

    @Override
    protected TaskEntity modelToEntity(GeaflowTask model) {
        TaskEntity task = super.modelToEntity(model);
        task.setType(model.getType());
        task.setStatus(model.getStatus());
        task.setStartTime(model.getStartTime());
        task.setEndTime(model.getEndTime());
        task.setReleaseId(model.getRelease().getId());
        task.setHaMetaConfigId(model.getHaMetaPluginConfig().getId());
        task.setDataConfigId(model.getDataPluginConfig().getId());
        task.setMetricConfigId(model.getMetricPluginConfig().getId());
        task.setRuntimeMetaConfigId(model.getRuntimeMetaPluginConfig().getId());
        task.setJobId(model.getRelease().getJob().getId());
        task.setToken(model.getToken());
        task.setHandle(Optional.ofNullable(model.getHandle()).map(JSON::toJSONString).orElse(null));
        task.setHost(model.getHost());
        return task;
    }

    @Override
    protected GeaflowTask entityToModel(TaskEntity entity) {
        GeaflowTask task = super.entityToModel(entity);
        task.setStatus(entity.getStatus());
        task.setType(entity.getType());
        task.setStartTime(entity.getStartTime());
        task.setEndTime(entity.getEndTime());
        task.setToken(entity.getToken());
        task.setHandle(GeaflowTaskHandle.parse(entity.getHandle()));
        task.setHost(entity.getHost());
        return task;
    }


    public GeaflowTask convert(TaskEntity entity, GeaflowRelease release,
                               GeaflowPluginConfig runtimeMetaPluginConfig,
                               GeaflowPluginConfig haMetaPluginConfig,
                               GeaflowPluginConfig metricPluginConfig,
                               GeaflowPluginConfig dataPluginConfig) {
        GeaflowTask task = this.entityToModel(entity);
        task.setRelease(release);
        task.setRuntimeMetaPluginConfig(runtimeMetaPluginConfig);
        task.setHaMetaPluginConfig(haMetaPluginConfig);
        task.setMetricPluginConfig(metricPluginConfig);
        task.setDataPluginConfig(dataPluginConfig);
        return task;
    }
}
