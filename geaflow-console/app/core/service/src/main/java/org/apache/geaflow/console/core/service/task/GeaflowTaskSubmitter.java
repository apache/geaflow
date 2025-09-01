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

import com.alibaba.fastjson.JSON;
import com.google.common.collect.Maps;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.geaflow.console.common.util.context.ContextHolder;
import org.apache.geaflow.console.common.util.exception.GeaflowException;
import org.apache.geaflow.console.common.util.type.GeaflowTaskStatus;
import org.apache.geaflow.console.core.model.GeaflowId;
import org.apache.geaflow.console.core.model.task.GeaflowTask;
import org.apache.geaflow.console.core.service.TaskService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class GeaflowTaskSubmitter {

    private static final ExecutorService EXECUTOR_SERVICE = new ThreadPoolExecutor(50, 500, 30, TimeUnit.SECONDS,
        new LinkedBlockingQueue<>(1000));

    private static final int JOB_SUBMIT_TIMEOUT_MINUTE = 4;

    @Autowired
    private TaskService taskService;

    @Autowired
    private GeaflowTaskOperator taskOperator;

    @Scheduled(cron = "0-59/10 * * * * ?")
    void submit() {
        try {
            ContextHolder.init();
            List<GeaflowId> taskIds = this.getWaitingTasks();
            if (CollectionUtils.isEmpty(taskIds)) {
                return;
            }

            log.info("task submitter start, task size: {}", taskIds.size());
            List<CompletableFuture<Void>> futureList = new ArrayList<>(taskIds.size());
            Map<String, GeaflowTask> submitMap = Maps.newConcurrentMap();

            for (GeaflowId taskId : taskIds) {
                futureList.add(CompletableFuture.runAsync(() -> {
                    try {
                        // set tenant and user by task
                        ContextHolder.init();
                        ContextHolder.get().setUserId(taskId.getModifierId());
                        ContextHolder.get().setTenantId(taskId.getTenantId());
                        GeaflowTask task = taskService.get(taskId.getId());
                        submitMap.put(task.getId(), task);

                        log.info("task {} submit start, curr status: {}", task.getId(), task.getStatus());

                        // update status to avoid submitting repeatedly.
                        boolean updateStatus = taskService.updateStatus(task.getId(), GeaflowTaskStatus.WAITING,
                            GeaflowTaskStatus.STARTING);
                        if (!updateStatus) {
                            throw new GeaflowException("task status has been changed, need {} status",
                                GeaflowTaskStatus.WAITING);
                        }
                        taskOperator.start(task);

                    } catch (Exception e) {
                        log.error("task {} submit error: {}", taskId.getId(), e.getMessage(), e);

                    } finally {
                        submitMap.remove(taskId.getId());
                        ContextHolder.destroy();
                    }

                }, EXECUTOR_SERVICE));
            }

            if (!futureList.isEmpty()) {
                try {
                    CompletableFuture.allOf(futureList.toArray(new CompletableFuture[futureList.size()]))
                        .get(JOB_SUBMIT_TIMEOUT_MINUTE, TimeUnit.MINUTES);
                } catch (Exception e) {
                    log.error("Task {} Submit Waiting Timeout", JSON.toJSONString(submitMap.keySet()), e);
                }
            }

        } finally {
            ContextHolder.destroy();
        }

    }

    private List<GeaflowId> getWaitingTasks() {
        return taskService.getTasksByStatus(GeaflowTaskStatus.WAITING);
    }
}
