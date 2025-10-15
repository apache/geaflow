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
import org.apache.geaflow.console.common.dal.entity.ChatEntity;
import org.apache.geaflow.console.common.dal.mapper.ChatMapper;
import org.apache.geaflow.console.common.dal.model.ChatSearch;
import org.apache.geaflow.console.common.util.context.ContextHolder;
import org.springframework.stereotype.Repository;

@Repository
public class ChatDao extends TenantLevelDao<ChatMapper, ChatEntity> implements IdDao<ChatEntity, ChatSearch> {

    public boolean dropByJobId(String jobId) {
        String userId = ContextHolder.get().getUserId();
        return lambdaUpdate()
            .eq(ChatEntity::getCreatorId, userId)
            .eq(ChatEntity::getJobId, jobId)
            .remove();
    }


    @Override
    public void configSearch(LambdaQueryWrapper<ChatEntity> wrapper, ChatSearch search) {
        wrapper.eq(search.getJobId() != null, ChatEntity::getJobId, search.getJobId());
        wrapper.eq(search.getModelId() != null, ChatEntity::getModelId, search.getModelId());
    }
}
