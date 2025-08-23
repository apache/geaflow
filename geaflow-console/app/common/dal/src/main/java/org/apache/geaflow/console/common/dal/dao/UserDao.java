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
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.geaflow.console.common.dal.entity.IdEntity;
import org.apache.geaflow.console.common.dal.entity.NameEntity;
import org.apache.geaflow.console.common.dal.entity.TenantUserMappingEntity;
import org.apache.geaflow.console.common.dal.entity.UserEntity;
import org.apache.geaflow.console.common.dal.mapper.UserMapper;
import org.apache.geaflow.console.common.dal.model.PageList;
import org.apache.geaflow.console.common.dal.model.UserSearch;
import org.springframework.stereotype.Repository;

@Repository
public class UserDao extends SystemLevelDao<UserMapper, UserEntity> implements NameDao<UserEntity, UserSearch> {

    public PageList<UserEntity> search(String tenantId, UserSearch search) {
        MPJLambdaWrapper<UserEntity> wrapper = new MPJLambdaWrapper<UserEntity>().selectAll(UserEntity.class)
            .innerJoin(TenantUserMappingEntity.class, TenantUserMappingEntity::getUserId, UserEntity::getId)
            .eq(TenantUserMappingEntity::getTenantId, tenantId);

        return search(wrapper, search);
    }

    @Override
    public void configJoinSearch(MPJLambdaWrapper<UserEntity> wrapper, UserSearch search) {
        String email = search.getEmail();
        String phone = search.getPhone();

        wrapper.like(StringUtils.isNotBlank(email), UserEntity::getEmail, email);
        wrapper.like(StringUtils.isNotBlank(phone), UserEntity::getPhone, phone);
    }

    public UserEntity getByToken(String token) {
        if (StringUtils.isBlank(token)) {
            return null;
        }

        return lambdaQuery().eq(UserEntity::getSessionToken, token).one();
    }

    public boolean existName(String name) {
        return lambdaQuery().eq(UserEntity::getName, name).exists();
    }

    public Map<String, String> getUserNames(Collection<String> userIds) {
        List<UserEntity> entities = lambdaQuery().select(UserEntity::getId, UserEntity::getName)
            .in(UserEntity::getId, userIds).list();
        return entities.stream().collect(Collectors.toMap(IdEntity::getId, NameEntity::getName));
    }
}
