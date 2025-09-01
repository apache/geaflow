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

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import org.apache.geaflow.console.common.dal.entity.IdEntity;
import org.apache.geaflow.console.common.dal.entity.JobEntity;
import org.apache.geaflow.console.common.dal.mapper.JobMapper;
import org.apache.geaflow.console.common.dal.model.JobSearch;
import org.springframework.stereotype.Repository;

@Repository
public class JobDao extends TenantLevelDao<JobMapper, JobEntity> implements NameDao<JobEntity, JobSearch> {

    @Override
    public void configSearch(LambdaQueryWrapper<JobEntity> wrapper, JobSearch search) {
        wrapper.eq(search.getJobType() != null, JobEntity::getType, search.getJobType());
        wrapper.eq(search.getInstanceId() != null, JobEntity::getInstanceId, search.getInstanceId());
    }

    public long getFileRefCount(String fileId, String excludeJobId) {
        return lambdaQuery().eq(JobEntity::getJarPackageId, fileId)
            .ne(excludeJobId != null, IdEntity::getId, excludeJobId)
            .count();
    }
}
