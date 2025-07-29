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

package org.apache.geaflow.console.core.service;

import com.google.common.base.Preconditions;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import org.apache.geaflow.console.common.dal.dao.NameDao;
import org.apache.geaflow.console.common.dal.dao.TenantUserMappingDao;
import org.apache.geaflow.console.common.dal.dao.UserDao;
import org.apache.geaflow.console.common.dal.entity.UserEntity;
import org.apache.geaflow.console.common.dal.model.PageList;
import org.apache.geaflow.console.common.dal.model.UserSearch;
import org.apache.geaflow.console.common.util.ListUtil;
import org.apache.geaflow.console.common.util.context.ContextHolder;
import org.apache.geaflow.console.common.util.exception.GeaflowIllegalException;
import org.apache.geaflow.console.core.model.security.GeaflowRole;
import org.apache.geaflow.console.core.model.security.GeaflowUser;
import org.apache.geaflow.console.core.service.converter.NameConverter;
import org.apache.geaflow.console.core.service.converter.UserConverter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class UserService extends NameService<GeaflowUser, UserEntity, UserSearch> {

    @Autowired
    private UserDao userDao;

    @Autowired
    private UserConverter userConverter;

    @Autowired
    private AuthorizationService authorizationService;

    @Autowired
    private TenantUserMappingDao tenantUserMappingDao;

    @Override
    protected NameDao<UserEntity, UserSearch> getDao() {
        return userDao;
    }

    @Override
    protected NameConverter<GeaflowUser, UserEntity> getConverter() {
        return userConverter;
    }

    @Override
    protected List<GeaflowUser> parse(List<UserEntity> entities) {
        return ListUtil.convert(entities, e -> userConverter.convert(e));
    }

    @Override
    public PageList<GeaflowUser> search(UserSearch search) {
        boolean systemSession = ContextHolder.get().isSystemSession();
        if (systemSession) {
            return super.search(search);
        }

        String tenantId = ContextHolder.get().getTenantId();
        return userDao.search(tenantId, search).transform(this::parse);
    }

    @Transactional
    public String createUser(GeaflowUser user) {
        if (userDao.existName(user.getName())) {
            throw new IllegalArgumentException("User exists");
        }

        // create user
        String userId = create(user);

        // init user id context when register success
        ContextHolder.get().setUserId(userId);

        // init the first user as system admin
        if (userDao.count() == 1) {
            authorizationService.addRole(null, userId, GeaflowRole.SYSTEM_ADMIN);
        }

        return userId;
    }

    public Map<String, String> getUserNames(Collection<String> userIds) {
        return userDao.getUserNames(userIds);
    }

    public boolean existTenantUser(String tenantId, String userId) {
        return tenantUserMappingDao.existUser(tenantId, userId);
    }

    public boolean addTenantUser(String tenantId, String userId) {
        if (tenantUserMappingDao.existUser(tenantId, userId)) {
            throw new GeaflowIllegalException("Tenant user exists");
        }

        Preconditions.checkNotNull(tenantId, "Invalid tenantId");
        Preconditions.checkArgument(userDao.exist(userId), "User not exists");
        return tenantUserMappingDao.addUser(tenantId, userId);
    }

    public boolean deleteTenantUser(String tenantId, String userId) {
        return tenantUserMappingDao.deleteUser(tenantId, userId);
    }
}

