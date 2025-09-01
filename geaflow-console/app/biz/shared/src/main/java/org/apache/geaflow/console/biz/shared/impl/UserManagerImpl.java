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

package org.apache.geaflow.console.biz.shared.impl;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import org.apache.geaflow.console.biz.shared.UserManager;
import org.apache.geaflow.console.biz.shared.convert.NameViewConverter;
import org.apache.geaflow.console.biz.shared.convert.UserViewConverter;
import org.apache.geaflow.console.biz.shared.view.UserView;
import org.apache.geaflow.console.common.dal.entity.UserEntity;
import org.apache.geaflow.console.common.dal.model.UserSearch;
import org.apache.geaflow.console.common.util.ListUtil;
import org.apache.geaflow.console.common.util.context.ContextHolder;
import org.apache.geaflow.console.common.util.exception.GeaflowIllegalException;
import org.apache.geaflow.console.core.model.security.GeaflowUser;
import org.apache.geaflow.console.core.service.NameService;
import org.apache.geaflow.console.core.service.UserService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class UserManagerImpl extends NameManagerImpl<GeaflowUser, UserView, UserSearch> implements UserManager {

    @Autowired
    private UserService userService;

    @Autowired
    private UserViewConverter userViewConverter;

    @Override
    protected NameViewConverter<GeaflowUser, UserView> getConverter() {
        return userViewConverter;
    }

    @Override
    protected List<GeaflowUser> parse(List<UserView> views) {
        return ListUtil.convert(views, userViewConverter::convert);
    }

    @Override
    protected NameService<GeaflowUser, UserEntity, UserSearch> getService() {
        return userService;
    }

    @Transactional
    @Override
    public String register(UserView view) {
        GeaflowUser user = userViewConverter.convert(view);
        return userService.createUser(user);
    }

    @Override
    public Map<String, String> getUserNames(Collection<String> userIds) {
        return userService.getUserNames(userIds);
    }

    @Override
    public UserView getUser(String userId) {
        boolean systemSession = ContextHolder.get().isSystemSession();
        String tenantId = ContextHolder.get().getTenantId();
        if (!systemSession && !userService.existTenantUser(tenantId, userId)) {
            throw new GeaflowIllegalException("User not found");
        }

        return get(userId);
    }

    @Override
    public String addUser(UserView view) {
        boolean systemSession = ContextHolder.get().isSystemSession();
        String tenantId = ContextHolder.get().getTenantId();
        if (systemSession) {
            throw new GeaflowIllegalException("Use user register instead");
        }

        String userName = view.getName();
        String userId = userService.getIdByName(userName);
        userService.addTenantUser(tenantId, userId);
        return userId;
    }

    @Override
    public boolean updateUser(String userId, UserView view) {
        if (!ContextHolder.get().getUserId().equals(userId)) {
            throw new GeaflowIllegalException("Change other user failed");
        }

        return updateById(userId, view);
    }

    @Override
    public boolean deleteUser(String userId) {
        boolean systemSession = ContextHolder.get().isSystemSession();
        String tenantId = ContextHolder.get().getTenantId();
        if (systemSession) {
            throw new GeaflowIllegalException("Permanently deleted user not allowed");
        }

        return userService.deleteTenantUser(tenantId, userId);
    }
}
