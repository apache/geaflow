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

import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.baomidou.mybatisplus.core.conditions.update.UpdateWrapper;
import org.apache.geaflow.console.common.dal.entity.UserLevelEntity;
import org.apache.geaflow.console.common.dal.mapper.GeaflowBaseMapper;
import org.apache.geaflow.console.common.util.context.ContextHolder;

public abstract class UserLevelDao<M extends GeaflowBaseMapper<E>, E extends UserLevelEntity> extends
    SystemLevelDao<M, E> {

    public static final String SYSTEM_FIELD_NAME = "`system`";

    @Override
    public void configQueryWrapper(QueryWrapper<E> wrapper) {
        String userId = ContextHolder.get().getUserId();
        wrapper.nested(
            w -> w.eq(SYSTEM_FIELD_NAME, true).or().eq(IdDao.CREATOR_FIELD_NAME, userId));
    }

    @Override
    public void configUpdateWrapper(UpdateWrapper<E> wrapper) {
        boolean systemSession = ContextHolder.get().isSystemSession();
        String userId = ContextHolder.get().getUserId();

        wrapper.eq(SYSTEM_FIELD_NAME, systemSession);
        if (!systemSession) {
            wrapper.eq(IdDao.CREATOR_FIELD_NAME, userId);
        }
    }

    public boolean validateGetId(String id) {
        if (id == null) {
            return false;
        }
        // system session can query all data
        return ContextHolder.get().isSystemSession() ? lambdaQuery().eq(E::getId, id).exists()
            : lambdaQuery().eq(E::isSystem, false).eq(E::getId, id).exists();
    }

    public boolean validateUpdateId(String id) {
        if (id == null) {
            return false;
        }
        // only update when system session or the current user is the creator.
        boolean systemSession = ContextHolder.get().isSystemSession();
        return lambdaQuery().eq(E::isSystem, systemSession).eq(E::getId, id).exists();
    }
}
