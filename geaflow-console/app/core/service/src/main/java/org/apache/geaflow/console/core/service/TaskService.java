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

package org.apache.geaflow.console.core.service;

import com.google.common.base.Preconditions;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.geaflow.console.common.dal.dao.IdDao;
import org.apache.geaflow.console.common.dal.dao.TaskDao;
import org.apache.geaflow.console.common.dal.entity.IdEntity;
import org.apache.geaflow.console.common.dal.entity.TaskEntity;
import org.apache.geaflow.console.common.dal.model.TaskSearch;
import org.apache.geaflow.console.common.util.ListUtil;
import org.apache.geaflow.console.common.util.exception.GeaflowException;
import org.apache.geaflow.console.common.util.type.GeaflowOperationType;
import org.apache.geaflow.console.common.util.type.GeaflowPluginCategory;
import org.apache.geaflow.console.common.util.type.GeaflowTaskStatus;
import org.apache.geaflow.console.core.model.GeaflowId;
import org.apache.geaflow.console.core.model.job.GeaflowJob;
import org.apache.geaflow.console.core.model.plugin.config.GeaflowPluginConfig;
import org.apache.geaflow.console.core.model.release.GeaflowRelease;
import org.apache.geaflow.console.core.model.runtime.GeaflowAudit;
import org.apache.geaflow.console.core.model.task.GeaflowTask;
import org.apache.geaflow.console.core.model.task.GeaflowTaskHandle;
import org.apache.geaflow.console.core.service.converter.IdConverter;
import org.apache.geaflow.console.core.service.converter.TaskConverter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class TaskService extends IdService<GeaflowTask, TaskEntity, TaskSearch> {

    @Autowired
    private TaskDao taskDao;

    @Autowired
    private TaskConverter taskConverter;

    @Autowired
    private ReleaseService releaseService;

    @Autowired
    private AuditService auditService;


    @Autowired
    private PluginService pluginService;

    @Autowired
    private PluginConfigService pluginConfigService;

    @Override
    protected IdDao<TaskEntity, TaskSearch> getDao() {
        return taskDao;
    }

    @Override
    protected IdConverter<GeaflowTask, TaskEntity> getConverter() {
        return taskConverter;
    }

    @Override
    protected List<GeaflowTask> parse(List<TaskEntity> taskEntities) {
        return taskEntities.stream().map(e -> {
            GeaflowRelease release = releaseService.get(e.getReleaseId());
            GeaflowPluginConfig runtimeConfig = pluginConfigService.get(e.getRuntimeMetaConfigId());
            GeaflowPluginConfig haMetaConfig = pluginConfigService.get(e.getHaMetaConfigId());
            GeaflowPluginConfig metricConfig = pluginConfigService.get(e.getMetricConfigId());
            GeaflowPluginConfig dataConfig = pluginConfigService.get(e.getDataConfigId());

            return taskConverter.convert(e, release, runtimeConfig, haMetaConfig, metricConfig, dataConfig);
        }).collect(Collectors.toList());
    }

    @Override
    public GeaflowTask get(String id) {
        GeaflowTask task = super.get(id);
        Preconditions.checkNotNull(task, "task %s not exist", id);
        return task;
    }

    public GeaflowTask getByJobId(String jobId) {
        List<TaskEntity> entities = taskDao.getByJobId(jobId);
        return entities.isEmpty() ? null : parse(entities).get(0);
    }

    public List<String> getIdsByJob(List<String> jobIds) {
        List<TaskEntity> entities = taskDao.getIdsByJobs(jobIds);
        return ListUtil.convert(entities, IdEntity::getId);
    }

    public List<GeaflowTask> createTask(GeaflowRelease release) {
        if (release.getReleaseVersion() != 1) {
            throw new GeaflowException("Job status is created or release version is not 1");
        }
        try {
            GeaflowTask task = new GeaflowTask();
            task.setStatus(GeaflowTaskStatus.CREATED);
            GeaflowJob job = release.getJob();
            task.setType(job.getType().getTaskType());
            task.setRelease(release);
            setPluginConfigs(task);
            create(task);
            auditService.create(new GeaflowAudit(task.getId(), GeaflowOperationType.CREATE));
            return Arrays.asList(task);
        } catch (Exception e) {
            throw new GeaflowException("Build task fail ", e);
        }
    }

    private void setPluginConfigs(GeaflowTask task) {
        GeaflowPluginCategory ha = GeaflowPluginCategory.HA_META;
        GeaflowPluginCategory metric = GeaflowPluginCategory.METRIC;
        GeaflowPluginCategory runtime = GeaflowPluginCategory.RUNTIME_META;
        GeaflowPluginCategory data = GeaflowPluginCategory.DATA;

        String haType = pluginService.getDefaultPlugin(ha).getType();
        String metricType = pluginService.getDefaultPlugin(metric).getType();
        String runtimeType = pluginService.getDefaultPlugin(runtime).getType();
        String dataType = pluginService.getDefaultPlugin(data).getType();

        GeaflowPluginConfig haConfig = pluginConfigService.getDefaultPluginConfig(ha, haType);
        GeaflowPluginConfig metricConfig = pluginConfigService.getDefaultPluginConfig(metric, metricType);
        GeaflowPluginConfig runtimeConfig = pluginConfigService.getDefaultPluginConfig(runtime, runtimeType);
        GeaflowPluginConfig dataConfig = pluginConfigService.getDefaultPluginConfig(data, dataType);

        task.setHaMetaPluginConfig(haConfig);
        task.setMetricPluginConfig(metricConfig);
        task.setRuntimeMetaPluginConfig(runtimeConfig);
        task.setDataPluginConfig(dataConfig);
    }

    public String bindRelease(GeaflowRelease release) {
        TaskEntity task = taskDao.getByJobId(release.getJob().getId()).get(0);
        if (task.getStatus() == GeaflowTaskStatus.CREATED && release.getReleaseVersion() == 1) {
            // don't need to bind at the first time
            return null;
        }
        task.setReleaseId(release.getId());
        boolean updateStatus = updateStatus(task.getId(), task.getStatus(), GeaflowTaskStatus.CREATED);
        if (!updateStatus) {
            throw new GeaflowException("task status has been changed");
        }
        taskDao.update(task);

        return task.getId();
    }

    public Map<String, GeaflowTaskHandle> getTaskHandles(GeaflowTaskStatus status) {
        List<TaskEntity> tasks = taskDao.getTasksByStatus(status);
        return ListUtil.toMap(tasks, IdEntity::getId, e -> GeaflowTaskHandle.parse(e.getHandle()));
    }

    public List<GeaflowId> getTasksByStatus(GeaflowTaskStatus status) {
        List<TaskEntity> tasks = taskDao.getTasksByStatus(status);
        return ListUtil.convert(tasks, this::convertToId);
    }

    public GeaflowId getByTaskToken(String token) {
        TaskEntity entity = taskDao.getByToken(token);
        return entity == null ? null : convertToId(entity);
    }

    private GeaflowId convertToId(TaskEntity entity) {
        GeaflowId model = new GeaflowTask();
        model.setTenantId(entity.getTenantId());
        model.setId(entity.getId());
        model.setCreatorId(entity.getCreatorId());
        model.setModifierId(entity.getModifierId());
        model.setGmtCreate(entity.getGmtCreate());
        model.setGmtModified(entity.getGmtModified());
        return model;
    }

    public boolean updateStatus(String taskId, GeaflowTaskStatus oldStatus, GeaflowTaskStatus newStatus) {
        if (oldStatus == newStatus) {
            return true;
        }
        return taskDao.updateStatus(taskId, oldStatus, newStatus);
    }

    public GeaflowTaskStatus getStatus(String id) {
        return Optional.ofNullable(taskDao.get(id)).map(TaskEntity::getStatus).orElse(null);
    }
}

