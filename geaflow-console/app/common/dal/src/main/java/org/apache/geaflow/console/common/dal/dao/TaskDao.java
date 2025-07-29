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

package org.apache.geaflow.console.common.dal.dao;

import com.github.yulichang.wrapper.MPJLambdaWrapper;
import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.apache.geaflow.console.common.dal.entity.JobEntity;
import org.apache.geaflow.console.common.dal.entity.JobResourceMappingEntity;
import org.apache.geaflow.console.common.dal.entity.ReleaseEntity;
import org.apache.geaflow.console.common.dal.entity.TaskEntity;
import org.apache.geaflow.console.common.dal.mapper.TaskMapper;
import org.apache.geaflow.console.common.dal.model.PageList;
import org.apache.geaflow.console.common.dal.model.TaskSearch;
import org.apache.geaflow.console.common.util.NetworkUtil;
import org.apache.geaflow.console.common.util.context.ContextHolder;
import org.apache.geaflow.console.common.util.type.GeaflowTaskStatus;
import org.springframework.stereotype.Repository;

@Repository
public class TaskDao extends TenantLevelDao<TaskMapper, TaskEntity> implements IdDao<TaskEntity, TaskSearch> {

    public List<TaskEntity> getByJobId(String jobId) {
        return lambdaQuery().eq(TaskEntity::getJobId, jobId).list();
    }

    public List<TaskEntity> getIdsByJobs(List<String> jobIds) {
        return lambdaQuery().select(TaskEntity::getId).in(TaskEntity::getJobId, jobIds).list();
    }

    public boolean updateStatus(String taskId, GeaflowTaskStatus oldStatus, GeaflowTaskStatus newStatus) {
        return lambdaUpdate().eq(TaskEntity::getId, taskId).eq(TaskEntity::getStatus, oldStatus)
            .ne(TaskEntity::getStatus, newStatus).set(TaskEntity::getStatus, newStatus).update();
    }

    @Override
    public boolean update(List<TaskEntity> entities) {
        entities.forEach(e -> e.setStatus(null));
        return IdDao.super.update(entities);
    }

    private MPJLambdaWrapper<TaskEntity> getJoinWrapper() {
        return new MPJLambdaWrapper<TaskEntity>().selectAll(TaskEntity.class)
            .innerJoin(ReleaseEntity.class, ReleaseEntity::getId, TaskEntity::getReleaseId)
            .innerJoin(JobEntity.class, JobEntity::getId, TaskEntity::getJobId)
            .leftJoin(JobResourceMappingEntity.class, JobResourceMappingEntity::getJobId, JobEntity::getId).distinct();
    }

    @Override
    public PageList<TaskEntity> search(TaskSearch search) {
        if (ContextHolder.get().isSystemSession()) {
            return systemSearch(search);
        } else {
            return search(getJoinWrapper(), search);
        }
    }

    public PageList<TaskEntity> systemSearch(TaskSearch search) {
        // don't select by tenantId
        MPJLambdaWrapper<TaskEntity> wrapper = getJoinWrapper();
        wrapper.comment(TenantLevelExtDao.IGNORE_TENANT_SIGNATURE);
        return search(wrapper, search);
    }


    @Override
    public void configJoinSearch(MPJLambdaWrapper<TaskEntity> wrapper, TaskSearch search) {
        wrapper.eq(search.getStatus() != null, TaskEntity::getStatus, search.getStatus());
        wrapper.eq(search.getHost() != null, TaskEntity::getHost, search.getHost());
        wrapper.eq(search.getVersionId() != null, ReleaseEntity::getVersionId, search.getVersionId());
        wrapper.eq(search.getClusterId() != null, ReleaseEntity::getClusterId, search.getClusterId());
        wrapper.eq(search.getJobType() != null, JobEntity::getType, search.getJobType());
        wrapper.eq(search.getJobId() != null, TaskEntity::getJobId, search.getJobId());
        wrapper.like(StringUtils.isNotBlank(search.getJobName()), JobEntity::getName, search.getJobName());

        wrapper.eq(search.getInstanceId() != null, JobEntity::getInstanceId, search.getInstanceId());
        wrapper.eq(search.getResourceName() != null, JobResourceMappingEntity::getResourceName, search.getResourceName());
        wrapper.eq(search.getResourceType() != null, JobResourceMappingEntity::getResourceType, search.getResourceType());

    }

    public List<TaskEntity> getTasksByStatus(GeaflowTaskStatus status) {
        return lambdaQuery().eq(TaskEntity::getStatus, status).eq(TaskEntity::getHost, NetworkUtil.getHostName())
            .comment(TenantLevelExtDao.IGNORE_TENANT_SIGNATURE).list();
    }

    public TaskEntity getByToken(String token) {
        return lambdaQuery().eq(TaskEntity::getToken, token).comment(TenantLevelExtDao.IGNORE_TENANT_SIGNATURE).one();
    }
}
