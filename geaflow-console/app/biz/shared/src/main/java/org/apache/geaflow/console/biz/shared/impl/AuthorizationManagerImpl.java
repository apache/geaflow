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

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.geaflow.console.biz.shared.AuthorizationManager;
import org.apache.geaflow.console.biz.shared.convert.AuthorizationViewConverter;
import org.apache.geaflow.console.biz.shared.convert.IdViewConverter;
import org.apache.geaflow.console.biz.shared.view.AuthorizationView;
import org.apache.geaflow.console.common.dal.model.AuthorizationSearch;
import org.apache.geaflow.console.common.util.ListUtil;
import org.apache.geaflow.console.common.util.context.ContextHolder;
import org.apache.geaflow.console.common.util.exception.GeaflowException;
import org.apache.geaflow.console.common.util.exception.GeaflowSecurityException;
import org.apache.geaflow.console.common.util.type.GeaflowRoleType;
import org.apache.geaflow.console.core.model.security.GeaflowAuthority;
import org.apache.geaflow.console.core.model.security.GeaflowAuthorization;
import org.apache.geaflow.console.core.model.security.GeaflowGrant;
import org.apache.geaflow.console.core.model.security.GeaflowRole;
import org.apache.geaflow.console.core.model.security.resource.GeaflowResource;
import org.apache.geaflow.console.core.service.AuthorizationService;
import org.apache.geaflow.console.core.service.IdService;
import org.apache.geaflow.console.core.service.UserService;
import org.apache.geaflow.console.core.service.security.ResourceFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class AuthorizationManagerImpl extends
    IdManagerImpl<GeaflowAuthorization, AuthorizationView, AuthorizationSearch> implements AuthorizationManager {

    @Autowired
    private AuthorizationService authorizationService;

    @Autowired
    private AuthorizationViewConverter authorizationViewConverter;

    @Autowired
    private UserService userService;

    @Autowired
    private ResourceFactory resourceFactory;

    @Override
    public List<GeaflowRoleType> getUserRoleTypes(String userId) {
        String tenantId = ContextHolder.get().getTenantId();
        return authorizationService.getUserRoleTypes(tenantId, userId);
    }

    @Override
    public void hasRole(GeaflowRole... roles) throws GeaflowSecurityException {
        if (!validateRoles(ContextHolder.get().getRoleTypes(), roles)) {
            throw new GeaflowSecurityException("User has no role of {}",
                Arrays.stream(roles).map(GeaflowRole::getType).collect(Collectors.toList()));
        }
    }

    @Override
    public void hasAuthority(GeaflowAuthority authority, GeaflowResource resource) throws GeaflowSecurityException {
        String userId = ContextHolder.get().getUserId();
        Set<GeaflowRoleType> roleTypes = ContextHolder.get().getRoleTypes();
        if (!validateAuthority(userId, roleTypes, authority, resource)) {
            throw new GeaflowSecurityException("User has no authority {} of {} {}", authority.getType(),
                resource.getType(), resource.getId());
        }
    }

    protected boolean validateRoles(Set<GeaflowRoleType> roleTypes, GeaflowRole[] expectRoles) {
        for (GeaflowRole expectRole : expectRoles) {
            while (expectRole != null) {
                if (roleTypes.contains(expectRole.getType())) {
                    return true;
                }
                expectRole = expectRole.getParent();
            }
        }

        return false;
    }

    protected boolean validateAuthority(String userId, Set<GeaflowRoleType> roleTypes, GeaflowAuthority authority,
                                        GeaflowResource resource) {
        // check role
        GeaflowGrant expectGrant = new GeaflowGrant(authority, resource);
        for (GeaflowGrant roleGrant : GeaflowRole.getGrants(roleTypes)) {
            if (roleGrant.include(expectGrant)) {
                return true;
            }
        }

        // check authorization
        while (resource != null) {
            if (authorizationService.exist(userId, authority, resource)) {
                return true;
            }

            resource = resource.getParent();
        }

        return false;
    }

    @Override
    protected IdService<GeaflowAuthorization, ?, AuthorizationSearch> getService() {
        return authorizationService;
    }

    @Override
    protected IdViewConverter<GeaflowAuthorization, AuthorizationView> getConverter() {
        return authorizationViewConverter;
    }

    @Override
    protected List<GeaflowAuthorization> parse(List<AuthorizationView> views) {
        return ListUtil.convert(views, e -> authorizationViewConverter.convert(e));
    }

    @Override
    public List<String> create(List<AuthorizationView> views) {
        for (AuthorizationView view : views) {
            String userId = view.getUserId();
            GeaflowAuthority authority = GeaflowAuthority.of(view.getAuthorityType());
            GeaflowResource resource = resourceFactory.build(view.getResourceType(), view.getResourceId());

            // check current user has the authority
            hasAuthority(authority, resource);
            // check the authored user has the authority
            Set<GeaflowRoleType> roleTypes = new HashSet<>(getUserRoleTypes(userId));
            if (validateAuthority(userId, roleTypes, authority, resource)) {
                String userName = userService.getUserNames(Collections.singleton(userId)).get(userId);
                throw new GeaflowException("User {} already has the authority of resource {} {}", userName,
                    resource.getType(), resource.getId());
            }
        }
        return super.create(views);
    }

    @Override
    public boolean drop(List<String> ids) {
        List<GeaflowAuthorization> authorizations = authorizationService.get(ids);
        for (GeaflowAuthorization authorization : authorizations) {
            GeaflowAuthority authority = GeaflowAuthority.of(authorization.getAuthorityType());
            GeaflowResource resource = resourceFactory.build(authorization.getResourceType(),
                authorization.getResourceId());

            // check current user has the authority
            hasAuthority(authority, resource);
        }
        return super.drop(ids);
    }
}
