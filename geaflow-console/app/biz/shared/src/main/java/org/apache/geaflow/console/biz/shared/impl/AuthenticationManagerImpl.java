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

import java.util.Set;
import org.apache.geaflow.console.biz.shared.AuthenticationManager;
import org.apache.geaflow.console.biz.shared.TenantManager;
import org.apache.geaflow.console.biz.shared.UserManager;
import org.apache.geaflow.console.biz.shared.convert.AuthenticationViewConverter;
import org.apache.geaflow.console.biz.shared.view.AuthenticationView;
import org.apache.geaflow.console.biz.shared.view.SessionView;
import org.apache.geaflow.console.biz.shared.view.TenantView;
import org.apache.geaflow.console.biz.shared.view.UserView;
import org.apache.geaflow.console.common.util.context.ContextHolder;
import org.apache.geaflow.console.common.util.type.GeaflowRoleType;
import org.apache.geaflow.console.core.model.security.GeaflowAuthentication;
import org.apache.geaflow.console.core.service.AuthenticationService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class AuthenticationManagerImpl implements AuthenticationManager {

    @Autowired
    private AuthenticationService authenticationService;

    @Autowired
    private AuthenticationViewConverter authenticationViewConverter;

    @Autowired
    private UserManager userManager;

    @Autowired
    private TenantManager tenantManager;

    @Override
    public AuthenticationView login(String loginName, String password, boolean systemLogin) {
        GeaflowAuthentication authentication = authenticationService.login(loginName, password, systemLogin);
        return authenticationViewConverter.convert(authentication);
    }

    @Override
    public AuthenticationView authenticate(String token) {
        GeaflowAuthentication authentication = authenticationService.authenticate(token);
        return authenticationViewConverter.convert(authentication);
    }

    @Override
    public SessionView currentSession() {
        String userId = ContextHolder.get().getUserId();
        String tenantId = ContextHolder.get().getTenantId();
        Set<GeaflowRoleType> roleTypes = ContextHolder.get().getRoleTypes();

        UserView user = userManager.getUser(userId);
        TenantView tenant = tenantId == null ? null : tenantManager.get(tenantId);
        GeaflowAuthentication authentication = authenticationService.getAuthenticationByUserId(userId);

        SessionView session = new SessionView();
        session.setUser(user);
        session.setTenant(tenant);
        session.setAuthentication(authenticationViewConverter.convert(authentication));
        session.setRoleTypes(roleTypes);
        return session;
    }

    @Override
    public boolean switchSession() {
        return authenticationService.switchSession();
    }

    @Override
    public boolean logout() {
        String token = ContextHolder.get().getSessionToken();
        return authenticationService.logout(token);
    }
}
