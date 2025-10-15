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

package org.apache.geaflow.console.biz.shared.impl;

import static org.apache.geaflow.console.common.util.type.GeaflowOperationType.DELETE;
import static org.apache.geaflow.console.common.util.type.GeaflowOperationType.FINISH;
import static org.apache.geaflow.console.common.util.type.GeaflowOperationType.STARTUP_NOTIFY;
import static org.apache.geaflow.console.common.util.type.GeaflowOperationType.STOP;
import static org.apache.geaflow.console.common.util.type.GeaflowTaskStatus.DELETED;
import static org.apache.geaflow.console.common.util.type.GeaflowTaskStatus.FAILED;
import static org.apache.geaflow.console.common.util.type.GeaflowTaskStatus.FINISHED;
import static org.apache.geaflow.console.common.util.type.GeaflowTaskStatus.RUNNING;
import static org.apache.geaflow.console.common.util.type.GeaflowTaskStatus.STOPPED;

import com.alibaba.fastjson2.JSON;
import java.io.File;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import javax.servlet.http.HttpServletResponse;
import lombok.extern.slf4j.Slf4j;
import org.apache.geaflow.console.biz.shared.TaskManager;
import org.apache.geaflow.console.biz.shared.convert.IdViewConverter;
import org.apache.geaflow.console.biz.shared.convert.TaskViewConverter;
import org.apache.geaflow.console.biz.shared.view.TaskStartupNotifyView;
import org.apache.geaflow.console.biz.shared.view.TaskView;
import org.apache.geaflow.console.common.dal.entity.TaskEntity;
import org.apache.geaflow.console.common.dal.model.PageList;
import org.apache.geaflow.console.common.dal.model.TaskSearch;
import org.apache.geaflow.console.common.util.FileUtil;
import org.apache.geaflow.console.common.util.Fmt;
import org.apache.geaflow.console.common.util.HTTPUtil;
import org.apache.geaflow.console.common.util.I18nUtil;
import org.apache.geaflow.console.common.util.NetworkUtil;
import org.apache.geaflow.console.common.util.context.ContextHolder;
import org.apache.geaflow.console.common.util.exception.GeaflowException;
import org.apache.geaflow.console.common.util.exception.GeaflowIllegalException;
import org.apache.geaflow.console.common.util.exception.GeaflowSecurityException;
import org.apache.geaflow.console.common.util.type.GeaflowOperationType;
import org.apache.geaflow.console.common.util.type.GeaflowPluginType;
import org.apache.geaflow.console.common.util.type.GeaflowTaskStatus;
import org.apache.geaflow.console.core.model.metric.GeaflowMetric;
import org.apache.geaflow.console.core.model.metric.GeaflowMetricMeta;
import org.apache.geaflow.console.core.model.metric.GeaflowMetricQueryRequest;
import org.apache.geaflow.console.core.model.runtime.GeaflowAudit;
import org.apache.geaflow.console.core.model.runtime.GeaflowCycle;
import org.apache.geaflow.console.core.model.runtime.GeaflowError;
import org.apache.geaflow.console.core.model.runtime.GeaflowOffset;
import org.apache.geaflow.console.core.model.runtime.GeaflowPipeline;
import org.apache.geaflow.console.core.model.task.GeaflowHeartbeatInfo;
import org.apache.geaflow.console.core.model.task.GeaflowTask;
import org.apache.geaflow.console.core.model.task.K8sTaskHandle;
import org.apache.geaflow.console.core.model.task.K8sTaskHandle.StartupNotifyInfo;
import org.apache.geaflow.console.core.model.task.TaskFile;
import org.apache.geaflow.console.core.service.AuditService;
import org.apache.geaflow.console.core.service.IdService;
import org.apache.geaflow.console.core.service.TaskService;
import org.apache.geaflow.console.core.service.config.DeployConfig;
import org.apache.geaflow.console.core.service.runtime.ContainerRuntime;
import org.apache.geaflow.console.core.service.task.GeaflowTaskOperator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Slf4j
@Service
public class TaskManagerImpl extends IdManagerImpl<GeaflowTask, TaskView, TaskSearch> implements TaskManager {

    @Autowired
    private TaskService taskService;

    @Autowired
    private TaskViewConverter taskViewConverter;

    @Autowired
    private GeaflowTaskOperator taskOperator;

    @Autowired
    private AuditService auditService;

    @Autowired
    private DeployConfig deployConfig;

    @Override
    public IdViewConverter<GeaflowTask, TaskView> getConverter() {
        return taskViewConverter;
    }

    @Override
    public IdService<GeaflowTask, TaskEntity, TaskSearch> getService() {
        return taskService;
    }

    @Override
    protected List<GeaflowTask> parse(List<TaskView> views) {
        throw new UnsupportedOperationException("Task can't be converted from view");
    }

    @Override
    @Transactional(rollbackFor = Exception.class)
    public void operate(String taskId, GeaflowOperationType action) {
        GeaflowTask task = taskService.get(taskId);
        task.getStatus().checkOperation(action);
        switch (action) {
            case START:
                start(task);
                break;
            case STOP:
                stop(task, false);
                break;
            case FINISH:
                stop(task, true);
                break;
            case REFRESH:
                taskOperator.refreshStatus(task);
                break;
            case RESET:
                clean(task);
                break;
            case DELETE:
                delete(task);
                break;
            default:
                throw new UnsupportedOperationException("not supported task action: " + action);
        }
    }


    protected void start(GeaflowTask task) {
        GeaflowTaskStatus status = task.getStatus();
        boolean updateStatus = taskService.updateStatus(task.getId(), status, GeaflowTaskStatus.WAITING);
        if (!updateStatus) {
            throw new GeaflowException("task status has been changed");
        }

        task.setHost(NetworkUtil.getHostName());
        taskService.update(task);
        log.info("submit task successfully, waiting for scheduling. id: {}", task.getId());
        auditService.create(new GeaflowAudit(task.getId(), GeaflowOperationType.START));
    }

    protected void stop(GeaflowTask task, boolean isFinish) {
        GeaflowTaskStatus status = task.getStatus();
        if (status == RUNNING) {
            taskOperator.stop(task);
        }

        if (isFinish) {
            taskService.updateStatus(task.getId(), status, FINISHED);
            auditService.create(new GeaflowAudit(task.getId(), FINISH));
            log.info("Task {} is finished by {}", task.getId(), task.getModifierId());
        } else {
            taskService.updateStatus(task.getId(), status, STOPPED);
            auditService.create(new GeaflowAudit(task.getId(), STOP));
            log.info("Task {} is stopped by {}", task.getId(), task.getModifierId());
        }
    }

    protected void clean(GeaflowTask task) {
        taskOperator.cleanMeta(task);
        taskOperator.cleanData(task);
        auditService.create(new GeaflowAudit(task.getId(), GeaflowOperationType.RESET));
    }

    protected void delete(GeaflowTask task) {
        clean(task);
        taskService.updateStatus(task.getId(), task.getStatus(), DELETED);
        auditService.create(new GeaflowAudit(task.getId(), DELETE));
    }

    @Override
    public GeaflowTaskStatus queryStatus(String taskId, Boolean refresh) {
        if (refresh != null && refresh) {
            return taskOperator.refreshStatus(taskService.get(taskId));
        }
        return taskService.getStatus(taskId);
    }

    @Override
    public PageList<GeaflowPipeline> queryPipelines(String taskId) {
        GeaflowTask task = taskService.get(taskId);
        return taskOperator.queryPipelines(task);
    }

    @Override
    public PageList<GeaflowCycle> queryCycles(String taskId, String pipelineId) {
        GeaflowTask task = taskService.get(taskId);
        return taskOperator.queryCycles(task, pipelineId);
    }

    @Override
    public PageList<GeaflowError> queryErrors(String taskId) {
        GeaflowTask task = taskService.get(taskId);
        return taskOperator.queryErrors(task);
    }

    @Override
    public PageList<GeaflowMetricMeta> queryMetricMeta(String taskId) {
        GeaflowTask task = taskService.get(taskId);
        return taskOperator.queryMetricMeta(task);
    }

    @Override
    public PageList<GeaflowMetric> queryMetrics(String taskId, GeaflowMetricQueryRequest queryRequest) {
        GeaflowTask task = taskService.get(taskId);
        return taskOperator.queryMetrics(task, queryRequest);
    }

    @Override
    public PageList<GeaflowOffset> queryOffsets(String taskId) {
        GeaflowTask task = taskService.get(taskId);
        return taskOperator.queryOffsets(task);
    }

    @Override
    public GeaflowHeartbeatInfo queryHeartbeat(String taskId) {
        GeaflowTask task = taskService.get(taskId);
        return taskOperator.queryHeartbeat(task);
    }

    @Transactional
    @Override
    public void startupNotify(String taskId, TaskStartupNotifyView startupNotifyView) {
        StartupNotifyInfo startupNotifyInfo;
        GeaflowTaskStatus newStatus;
        GeaflowTask task = taskService.get(taskId);
        if (task.getHandle().getClusterType() != GeaflowPluginType.K8S) {
            return;
        }

        if (startupNotifyView.isSuccess()) {
            startupNotifyInfo = startupNotifyView.getData();
            newStatus = RUNNING;
        } else {
            startupNotifyInfo = new StartupNotifyInfo();
            newStatus = FAILED;
        }
        ((K8sTaskHandle) task.getHandle()).setStartupNotifyInfo(startupNotifyInfo);
        taskService.update(task);
        taskService.updateStatus(task.getId(), task.getStatus(), newStatus);
        log.info("Task {} get startup notify '{}' from cluster", task.getId(), JSON.toJSONString(startupNotifyView));
        auditService.create(new GeaflowAudit(taskId, STARTUP_NOTIFY, "Task startup success"));
    }

    @Override
    public void download(String taskId, String path, HttpServletResponse response) {
        // check task id
        GeaflowTask task = taskService.get(taskId);
        if (task == null) {
            throw new GeaflowException("Invalid task id {}", taskId);
        }

        // check task token and deploy mode
        if (!taskId.equals(ContextHolder.get().getTaskId()) || !deployConfig.isLocalMode()) {
            throw new GeaflowSecurityException("Download task {} file {} is not allowed", taskId, path);
        }

        // check file used by task
        String gatewayUrl = deployConfig.getGatewayUrl();
        String taskFileUrl = task.getTaskFileUrl(gatewayUrl, path);
        List<TaskFile> files = new ArrayList<>();
        files.addAll(task.getVersionFiles(gatewayUrl));
        files.addAll(task.getUserFiles(gatewayUrl));
        if (files.stream().noneMatch(f -> f.getUrl().equals(taskFileUrl))) {
            throw new GeaflowIllegalException("Invalid task file {}", path);
        }

        // download local file
        String name = new File(path).getName();
        try (InputStream input = FileUtil.readFileStream(path)) {
            HTTPUtil.download(response, input, name);

        } catch (Exception e) {
            throw new GeaflowException("Download file {} from {} failed", name, path, e);
        }
    }

    @Override
    public String getLogs(String taskId) {
        GeaflowTask task = taskService.get(taskId);
        GeaflowPluginType type = task.getRelease().getCluster().getType();
        if (type.equals(GeaflowPluginType.CONTAINER)) {
            String logFilePath = ContainerRuntime.getLogFilePath(taskId);
            return Fmt.as(I18nUtil.getMessage("i18n.key.container.task.log.tips"), logFilePath);
        } else {
            return Fmt.as(I18nUtil.getMessage("i18n.key.k8s.task.log.tips"));
        }
    }


    @Override
    public TaskView getByJobId(String jobId) {
        return build(taskService.getByJobId(jobId));
    }

    @Override
    public boolean drop(List<String> ids) {
        for (String id : ids) {
            operate(id, DELETE);
        }

        return super.drop(ids);
    }

}
