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
import java.util.Date;
import java.util.List;
import org.apache.geaflow.console.common.dal.dao.UserDao;
import org.apache.geaflow.console.common.dal.entity.UserEntity;
import org.apache.geaflow.console.common.util.Md5Util;
import org.apache.geaflow.console.common.util.context.ContextHolder;
import org.apache.geaflow.console.common.util.exception.GeaflowException;
import org.apache.geaflow.console.common.util.exception.GeaflowSecurityException;
import org.apache.geaflow.console.core.model.security.GeaflowAuthentication;
import org.apache.geaflow.console.core.model.security.GeaflowRole;
import org.apache.geaflow.console.core.model.security.GeaflowTenant;
import org.apache.geaflow.console.core.model.security.GeaflowUser;
import org.apache.geaflow.console.core.service.security.TokenGenerator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class AuthenticationService {

    private static final int TOKEN_EXPIRED_SECONDS = 24 * 60 * 60;

    @Autowired
    private UserDao userDao;

    @Autowired
    private TokenGenerator tokenGenerator;

    @Autowired
    private TenantService tenantService;

    @Autowired
    private UserService userService;

    @Autowired
    private InstanceService instanceService;

    @Autowired
    private AuthorizationService authorizationService;

    @Transactional
    public GeaflowAuthentication login(String loginName, String password, boolean systemLogin) {
        Preconditions.checkNotNull(loginName, "Invalid loginName");
        Preconditions.checkNotNull(password, "Invalid password");

        // get login user
        UserEntity entity = userDao.getByName(loginName);
        Preconditions.checkNotNull(entity, "User not found");

        try {
            String userId = entity.getId();

            // check password
            if (!Md5Util.encodeString(password).equals(entity.getPasswordSign())) {
                throw new GeaflowException("Invalid password");
            }

            // check system admin role
            if (systemLogin && !authorizationService.existRole(null, userId, GeaflowRole.SYSTEM_ADMIN)) {
                throw new GeaflowException("Not system admin");
            }

            // create or reuse authentication
            GeaflowAuthentication authentication = convert(entity);
            if (!authentication.isExpired(TOKEN_EXPIRED_SECONDS) && authentication.isSystemSession() == systemLogin) {
                updateAccessTime(userId);

            } else {
                createAuthentication(userId, systemLogin);
            }

            // init user id context when authentication success
            ContextHolder.get().setUserId(userId);

            // init tenant and instance when first login
            ensureTenantInited(userId);

            // get authentication
            return getAuthenticationByUserId(userId);

        } catch (Exception e) {
            throw new GeaflowSecurityException("User login failed", e);
        }
    }

    public GeaflowAuthentication authenticate(String token) {
        try {
            GeaflowAuthentication authentication = getAuthenticationByToken(token);
            if (authentication.isExpired(TOKEN_EXPIRED_SECONDS)) {
                throw new GeaflowException("User token expired");
            }

            String userId = authentication.getUserId();
            updateAccessTime(userId);

            return getAuthenticationByUserId(userId);

        } catch (Exception e) {
            throw new GeaflowSecurityException("User authenticate failed", e);
        }
    }

    public boolean switchSession() {
        String userId = ContextHolder.get().getUserId();
        boolean expectSystemSession = !ContextHolder.get().isSystemSession();

        // check system admin role
        if (expectSystemSession && !authorizationService.existRole(null, userId, GeaflowRole.SYSTEM_ADMIN)) {
            throw new GeaflowException("Not system admin");
        }

        return updateSystemSession(userId, expectSystemSession);
    }

    public boolean logout(String token) {
        try {
            GeaflowAuthentication authentication = getAuthenticationByToken(token);
            if (authentication.isExpired(TOKEN_EXPIRED_SECONDS)) {
                return true;
            }

            return destroyAuthentication(authentication.getUserId());

        } catch (Exception e) {
            throw new GeaflowSecurityException("User logout failed", e);
        }
    }

    private void ensureTenantInited(String userId) {
        if (ContextHolder.get().getTenantId() != null) {
            return;
        }

        List<GeaflowTenant> userTenants = tenantService.getUserTenants(userId);
        if (!userTenants.isEmpty()) {
            return;
        }

        GeaflowUser user = userService.get(userId);

        // create tenant
        String tenantId = tenantService.createDefaultTenant(user);

        // add user to new tenant
        userService.addTenantUser(tenantId, userId);

        // init user as tenant admin
        authorizationService.addRole(tenantId, userId, GeaflowRole.TENANT_ADMIN);

        // activate user tenant
        tenantService.activateTenant(tenantId, userId);

        // create instance
        instanceService.createDefaultInstance(tenantId, user);
    }

    public GeaflowAuthentication getAuthenticationByUserId(String userId) {
        UserEntity entity = userDao.get(userId);
        Preconditions.checkNotNull(entity, "Invalid token");
        return convert(entity);
    }

    public GeaflowAuthentication getAuthenticationByToken(String token) {
        UserEntity entity = userDao.getByToken(token);
        Preconditions.checkNotNull(entity, "Invalid token %s", token);
        return convert(entity);
    }

    public boolean createAuthentication(String userId, boolean systemLogin) {
        UserEntity entity = new UserEntity();
        entity.setId(userId);
        entity.setSessionToken(tokenGenerator.nextToken());
        entity.setSystemSession(systemLogin);
        entity.setAccessTime(new Date());
        return userDao.updateById(entity);
    }

    public boolean updateAccessTime(String userId) {
        return userDao.lambdaUpdate().set(UserEntity::getAccessTime, new Date()).eq(UserEntity::getId, userId).update();
    }

    public boolean updateSystemSession(String userId, boolean systemSession) {
        return userDao.lambdaUpdate().set(UserEntity::isSystemSession, systemSession).eq(UserEntity::getId, userId)
            .update();
    }

    public boolean destroyAuthentication(String userId) {
        return userDao.lambdaUpdate().set(UserEntity::getSessionToken, null).set(UserEntity::isSystemSession, false)
            .set(UserEntity::getAccessTime, null).eq(UserEntity::getId, userId).update();
    }

    private GeaflowAuthentication convert(UserEntity entity) {
        if (entity == null) {
            return null;
        }

        GeaflowAuthentication authentication = new GeaflowAuthentication();
        authentication.setUserId(entity.getId());
        authentication.setSessionToken(entity.getSessionToken());
        authentication.setSystemSession(entity.isSystemSession());
        authentication.setAccessTime(entity.getAccessTime());
        return authentication;
    }
}

