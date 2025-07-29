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
import java.util.List;
import org.apache.geaflow.console.common.dal.entity.StatementEntity;
import org.apache.geaflow.console.common.dal.mapper.StatementMapper;
import org.apache.geaflow.console.common.dal.model.StatementSearch;
import org.springframework.stereotype.Repository;

@Repository
public class StatementDao extends TenantLevelDao<StatementMapper, StatementEntity>
    implements IdDao<StatementEntity, StatementSearch> {

    @Override
    public void configSearch(LambdaQueryWrapper<StatementEntity> wrapper, StatementSearch search) {
        wrapper.eq(search.getJobId() != null, StatementEntity::getJobId, search.getJobId())
            .eq(search.getStatus() != null, StatementEntity::getStatus, search.getStatus());
    }

    public boolean dropByJobIds(List<String> jobIds) {
        return lambdaUpdate().in(StatementEntity::getJobId, jobIds).remove();
    }
}
