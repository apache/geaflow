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

import java.util.ArrayList;
import java.util.List;
import org.apache.commons.collections.CollectionUtils;
import org.apache.geaflow.console.common.dal.entity.JobResourceMappingEntity;
import org.apache.geaflow.console.common.dal.mapper.JobResourceMappingMapper;
import org.apache.geaflow.console.common.dal.model.IdSearch;
import org.apache.geaflow.console.common.util.type.GeaflowResourceType;
import org.springframework.stereotype.Repository;

@Repository
public class JobResourceMappingDao extends TenantLevelDao<JobResourceMappingMapper, JobResourceMappingEntity> implements
    IdDao<JobResourceMappingEntity, IdSearch> {

    public List<JobResourceMappingEntity> getResourcesByJobId(String jobId, GeaflowResourceType resourceType) {
        if (jobId == null) {
            return new ArrayList<>();
        }

        return lambdaQuery().eq(JobResourceMappingEntity::getJobId, jobId)
            .eq(JobResourceMappingEntity::getResourceType, resourceType).list();
    }

    public List<JobResourceMappingEntity> getJobByResources(String resourceName, String instanceId, GeaflowResourceType resourceType) {
        if (resourceName == null || instanceId == null) {
            return new ArrayList<>();
        }

        return lambdaQuery()
            .eq(JobResourceMappingEntity::getResourceName, resourceName)
            .eq(JobResourceMappingEntity::getInstanceId, instanceId)
            .eq(JobResourceMappingEntity::getResourceType, resourceType).list();
    }

    public boolean dropByJobIds(List<String> jobIds) {
        if (CollectionUtils.isEmpty(jobIds)) {
            return true;
        }

        return lambdaUpdate().in(JobResourceMappingEntity::getJobId, jobIds).remove();

    }

    public void removeJobResources(List<JobResourceMappingEntity> entities) {
        for (JobResourceMappingEntity entity : entities) {
            lambdaUpdate().eq(JobResourceMappingEntity::getResourceName, entity.getResourceName())
                .eq(JobResourceMappingEntity::getInstanceId, entity.getInstanceId())
                .eq(JobResourceMappingEntity::getResourceType, entity.getResourceType())
                .eq(JobResourceMappingEntity::getJobId, entity.getJobId()).remove();
        }
    }
}
