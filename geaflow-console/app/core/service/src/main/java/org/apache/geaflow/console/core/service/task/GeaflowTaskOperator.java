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

package org.apache.geaflow.console.core.service.task;

import static org.apache.geaflow.console.common.util.type.GeaflowTaskStatus.FAILED;
import static org.apache.geaflow.console.common.util.type.GeaflowTaskStatus.RUNNING;
import static org.apache.geaflow.console.common.util.type.GeaflowTaskStatus.STARTING;

import com.alibaba.fastjson.JSON;
import java.util.Date;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.apache.geaflow.console.common.dal.model.PageList;
import org.apache.geaflow.console.common.util.DateTimeUtil;
import org.apache.geaflow.console.common.util.Fmt;
import org.apache.geaflow.console.common.util.type.GeaflowOperationType;
import org.apache.geaflow.console.common.util.type.GeaflowPluginType;
import org.apache.geaflow.console.common.util.type.GeaflowTaskStatus;
import org.apache.geaflow.console.core.model.metric.GeaflowMetric;
import org.apache.geaflow.console.core.model.metric.GeaflowMetricMeta;
import org.apache.geaflow.console.core.model.metric.GeaflowMetricQueryRequest;
import org.apache.geaflow.console.core.model.plugin.config.GeaflowPluginConfig;
import org.apache.geaflow.console.core.model.runtime.GeaflowAudit;
import org.apache.geaflow.console.core.model.runtime.GeaflowCycle;
import org.apache.geaflow.console.core.model.runtime.GeaflowError;
import org.apache.geaflow.console.core.model.runtime.GeaflowOffset;
import org.apache.geaflow.console.core.model.runtime.GeaflowPipeline;
import org.apache.geaflow.console.core.model.task.GeaflowHeartbeatInfo;
import org.apache.geaflow.console.core.model.task.GeaflowHeartbeatInfo.ContainerInfo;
import org.apache.geaflow.console.core.model.task.GeaflowTask;
import org.apache.geaflow.console.core.model.task.GeaflowTaskHandle;
import org.apache.geaflow.console.core.model.task.K8sTaskHandle;
import org.apache.geaflow.console.core.service.AuditService;
import org.apache.geaflow.console.core.service.TaskService;
import org.apache.geaflow.console.core.service.runtime.GeaflowRuntime;
import org.apache.geaflow.console.core.service.runtime.RuntimeFactory;
import org.apache.geaflow.console.core.service.security.TokenGenerator;
import org.apache.geaflow.console.core.service.store.GeaflowDataStore;
import org.apache.geaflow.console.core.service.store.GeaflowHaMetaStore;
import org.apache.geaflow.console.core.service.store.GeaflowMetricStore;
import org.apache.geaflow.console.core.service.store.GeaflowRuntimeMetaStore;
import org.apache.geaflow.console.core.service.store.factory.DataStoreFactory;
import org.apache.geaflow.console.core.service.store.factory.HaMetaStoreFactory;
import org.apache.geaflow.console.core.service.store.factory.MetricStoreFactory;
import org.apache.geaflow.console.core.service.store.factory.RuntimeMetaStoreFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class GeaflowTaskOperator {

    private static final int TASK_STARTUP_TIMEOUT = 8 * 60;

    @Autowired
    private TaskService taskService;

    @Autowired
    private TokenGenerator tokenGenerator;

    @Autowired
    private RuntimeFactory runtimeFactory;

    @Autowired
    private RuntimeMetaStoreFactory runtimeMetaStoreFactory;

    @Autowired
    private MetricStoreFactory metricStoreFactory;

    @Autowired
    private HaMetaStoreFactory haMetaStoreFactory;

    @Autowired
    private DataStoreFactory dataStoreFactory;

    @Autowired
    private AuditService auditService;

    public boolean start(GeaflowTask task) {
        GeaflowRuntime runtime = runtimeFactory.getRuntime(task);

        // generate task token and save before start
        task.setToken(tokenGenerator.nextTaskToken());
        taskService.update(task);

        // submit job to the engine
        try {
            GeaflowTaskHandle handle = runtime.start(task);
            task.setHandle(handle);
            task.setStartTime(new Date());
            taskService.update(task);
            taskService.updateStatus(task.getId(), STARTING, RUNNING);
            log.info("Submit task {} successfully", task.getId());
            return true;

        } catch (Exception e) {
            log.error("Submit task {} failed", task.getId(), e);
            taskService.updateStatus(task.getId(), STARTING, FAILED);
            throw e;
        }
    }

    public void stop(GeaflowTask task) {
        runtimeFactory.getRuntime(task).stop(task);
        log.info("Stop task {} success, handle={}", task.getId(), JSON.toJSONString(task.getHandle()));
    }

    public GeaflowTaskStatus refreshStatus(GeaflowTask task) {
        GeaflowTaskStatus oldStatus = task.getStatus();

        // only refresh running task
        if (!RUNNING.equals(oldStatus)) {
            return oldStatus;
        }

        GeaflowTaskStatus newStatus = runtimeFactory.getRuntime(task).queryStatus(task);
        if (newStatus == FAILED && task.getRelease().getCluster().getType() == GeaflowPluginType.K8S) {
            // task has not been started completely
            if (!Optional.ofNullable(((K8sTaskHandle) task.getHandle())).map(K8sTaskHandle::getStartupNotifyInfo)
                .isPresent()) {
                if (DateTimeUtil.isExpired(task.getStartTime(), TASK_STARTUP_TIMEOUT)) {
                    // release task resource
                    this.stop(task);

                    // waiting startup timeout
                    String detail = Fmt.as("Waiting task startup timeout after {}s", TASK_STARTUP_TIMEOUT);
                    log.info(detail);
                    auditService.create(new GeaflowAudit(task.getId(), GeaflowOperationType.STOP, detail));

                } else {
                    // waiting startup, keep status not changed
                    newStatus = oldStatus;
                }
            }
        }

        taskService.updateStatus(task.getId(), oldStatus, newStatus);
        return newStatus;
    }

    public boolean cleanMeta(GeaflowTask task) {
        GeaflowPluginConfig runtimeMetaConfig = task.getRuntimeMetaPluginConfig();
        GeaflowPluginConfig haMetaConfig = task.getHaMetaPluginConfig();
        GeaflowRuntimeMetaStore runtimeMetaStore = runtimeMetaStoreFactory.getRuntimeMetaStore(runtimeMetaConfig);
        GeaflowHaMetaStore haMetaStore = haMetaStoreFactory.getHaMetaStore(haMetaConfig);
        runtimeMetaStore.cleanRuntimeMeta(task);
        haMetaStore.cleanHaMeta(task);
        return true;
    }

    public boolean cleanData(GeaflowTask task) {
        GeaflowPluginConfig dataConfig = task.getDataPluginConfig();
        GeaflowDataStore dataStore = dataStoreFactory.getDataStore(dataConfig.getType());
        dataStore.cleanTaskData(task);
        return true;
    }

    public PageList<GeaflowPipeline> queryPipelines(GeaflowTask task) {
        GeaflowPluginConfig runtimeMetaConfig = task.getRuntimeMetaPluginConfig();
        GeaflowRuntimeMetaStore runtimeMetaStore = runtimeMetaStoreFactory.getRuntimeMetaStore(runtimeMetaConfig);
        return runtimeMetaStore.queryPipelines(task);
    }

    public PageList<GeaflowCycle> queryCycles(GeaflowTask task, String pipelineId) {
        GeaflowPluginConfig runtimeMetaConfig = task.getRuntimeMetaPluginConfig();
        GeaflowRuntimeMetaStore runtimeMetaStore = runtimeMetaStoreFactory.getRuntimeMetaStore(runtimeMetaConfig);
        return runtimeMetaStore.queryCycles(task, pipelineId);
    }

    public PageList<GeaflowMetricMeta> queryMetricMeta(GeaflowTask task) {
        GeaflowPluginConfig runtimeMetaConfig = task.getRuntimeMetaPluginConfig();
        GeaflowRuntimeMetaStore runtimeMetaStore = runtimeMetaStoreFactory.getRuntimeMetaStore(runtimeMetaConfig);
        return runtimeMetaStore.queryMetricMeta(task);
    }

    public PageList<GeaflowMetric> queryMetrics(GeaflowTask task, GeaflowMetricQueryRequest queryRequest) {
        GeaflowPluginConfig metricConfig = task.getMetricPluginConfig();
        GeaflowMetricStore metaStore = metricStoreFactory.getMetricStore(metricConfig);
        return metaStore.queryMetrics(task, queryRequest);
    }

    public PageList<GeaflowOffset> queryOffsets(GeaflowTask task) {
        GeaflowPluginConfig runtimeMetaConfig = task.getRuntimeMetaPluginConfig();
        GeaflowRuntimeMetaStore runtimeMetaStore = runtimeMetaStoreFactory.getRuntimeMetaStore(runtimeMetaConfig);
        return runtimeMetaStore.queryOffsets(task);
    }

    public PageList<GeaflowError> queryErrors(GeaflowTask task) {
        GeaflowPluginConfig runtimeMetaConfig = task.getRuntimeMetaPluginConfig();
        GeaflowRuntimeMetaStore runtimeMetaStore = runtimeMetaStoreFactory.getRuntimeMetaStore(runtimeMetaConfig);
        return runtimeMetaStore.queryErrors(task);
    }

    public GeaflowHeartbeatInfo queryHeartbeat(GeaflowTask task) {
        GeaflowPluginConfig runtimeMetaConfig = task.getRuntimeMetaPluginConfig();
        GeaflowRuntimeMetaStore runtimeMetaStore = runtimeMetaStoreFactory.getRuntimeMetaStore(runtimeMetaConfig);
        GeaflowHeartbeatInfo heartbeatInfo = runtimeMetaStore.queryHeartbeat(task);
        setupHeartbeatInfo(heartbeatInfo);
        return heartbeatInfo;
    }

    private void setupHeartbeatInfo(GeaflowHeartbeatInfo heartbeatInfo) {
        if (heartbeatInfo != null) {
            int activeNum = 0;
            long now = System.currentTimeMillis();
            long expiredTime = now - heartbeatInfo.getExpiredTimeMs();
            for (ContainerInfo container : heartbeatInfo.getContainers()) {
                if (container.getLastTimestamp() != null && container.getLastTimestamp() > expiredTime) {
                    container.setActive(true);
                    activeNum++;
                }
            }
            heartbeatInfo.setActiveNum(activeNum);
        }
    }
}
