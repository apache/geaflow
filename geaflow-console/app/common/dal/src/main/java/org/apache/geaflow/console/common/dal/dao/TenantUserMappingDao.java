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
import org.apache.geaflow.console.common.dal.entity.TenantEntity;
import org.apache.geaflow.console.common.dal.entity.TenantUserMappingEntity;
import org.apache.geaflow.console.common.dal.mapper.TenantUserMappingMapper;
import org.apache.geaflow.console.common.dal.model.IdSearch;
import org.springframework.stereotype.Repository;

@Repository
public class TenantUserMappingDao extends SystemLevelDao<TenantUserMappingMapper, TenantUserMappingEntity> implements
    IdDao<TenantUserMappingEntity, IdSearch> {

    public boolean existUser(String tenantId, String userId) {
        return lambdaQuery().eq(TenantUserMappingEntity::getTenantId, tenantId)
            .eq(TenantUserMappingEntity::getUserId, userId).exists();
    }

    public boolean addUser(String tenantId, String userId) {
        TenantUserMappingEntity entity = new TenantUserMappingEntity();
        entity.setTenantId(tenantId);
        entity.setUserId(userId);
        return StringUtils.isNotBlank(create(entity));
    }

    public boolean deleteUser(String tenantId, String userId) {
        return lambdaUpdate().eq(TenantUserMappingEntity::getTenantId, tenantId)
            .eq(TenantUserMappingEntity::getUserId, userId).remove();
    }

    public List<TenantEntity> getUserTenants(String userId) {
        MPJLambdaWrapper<TenantUserMappingEntity> wrapper = new MPJLambdaWrapper<TenantUserMappingEntity>().selectAll(
                TenantEntity.class).innerJoin(TenantEntity.class, TenantEntity::getId,
                TenantUserMappingEntity::getTenantId)
            .eq(TenantUserMappingEntity::getUserId, userId);
        return getBaseMapper().selectJoinList(TenantEntity.class, wrapper);
    }

    public TenantEntity getUserActiveTenant(String userId) {
        MPJLambdaWrapper<TenantUserMappingEntity> wrapper = new MPJLambdaWrapper<TenantUserMappingEntity>().selectAll(
                TenantEntity.class).innerJoin(TenantEntity.class, TenantEntity::getId,
                TenantUserMappingEntity::getTenantId)
            .eq(TenantUserMappingEntity::getUserId, userId).eq(TenantUserMappingEntity::isActive, true);
        return getBaseMapper().selectJoinOne(TenantEntity.class, wrapper);
    }

    public void activateUserTenant(String tenantId, String userId) {
        lambdaUpdate().set(TenantUserMappingEntity::isActive, true).eq(TenantUserMappingEntity::getTenantId, tenantId)
            .eq(TenantUserMappingEntity::getUserId, userId).update();
    }

    public void deactivateUserTenants(String userId) {
        lambdaUpdate().set(TenantUserMappingEntity::isActive, false).eq(TenantUserMappingEntity::getUserId, userId)
            .update();
    }
}
