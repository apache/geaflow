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

import java.util.List;
import java.util.stream.Collectors;
import org.apache.geaflow.console.common.dal.dao.AuthorizationDao;
import org.apache.geaflow.console.common.dal.dao.IdDao;
import org.apache.geaflow.console.common.dal.dao.UserRoleMappingDao;
import org.apache.geaflow.console.common.dal.entity.AuthorizationEntity;
import org.apache.geaflow.console.common.dal.model.AuthorizationSearch;
import org.apache.geaflow.console.common.util.ListUtil;
import org.apache.geaflow.console.common.util.exception.GeaflowIllegalException;
import org.apache.geaflow.console.common.util.type.GeaflowAuthorityType;
import org.apache.geaflow.console.common.util.type.GeaflowResourceType;
import org.apache.geaflow.console.common.util.type.GeaflowRoleType;
import org.apache.geaflow.console.core.model.security.GeaflowAuthority;
import org.apache.geaflow.console.core.model.security.GeaflowAuthorization;
import org.apache.geaflow.console.core.model.security.GeaflowRole;
import org.apache.geaflow.console.core.model.security.resource.GeaflowResource;
import org.apache.geaflow.console.core.service.converter.AuthorizationConverter;
import org.apache.geaflow.console.core.service.converter.IdConverter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class AuthorizationService extends IdService<GeaflowAuthorization, AuthorizationEntity, AuthorizationSearch> {

    @Autowired
    private AuthorizationDao authorizationDao;

    @Autowired
    private AuthorizationConverter authorizationConverter;

    @Autowired
    private UserRoleMappingDao userRoleMappingDao;

    @Override
    protected IdDao<AuthorizationEntity, AuthorizationSearch> getDao() {
        return authorizationDao;
    }

    @Override
    protected IdConverter<GeaflowAuthorization, AuthorizationEntity> getConverter() {
        return authorizationConverter;
    }

    @Override
    protected List<GeaflowAuthorization> parse(List<AuthorizationEntity> grantEntities) {
        return grantEntities.stream().map(e -> authorizationConverter.convert(e)).collect(Collectors.toList());
    }

    public List<GeaflowRoleType> getUserRoleTypes(String tenantId, String userId) {
        return userRoleMappingDao.getRoleTypes(tenantId, userId);
    }

    public boolean existRole(String tenantId, String userId, GeaflowRole role) {
        return userRoleMappingDao.existRoleType(tenantId, userId, role.getType());
    }

    public void addRole(String tenantId, String userId, GeaflowRole role) {
        GeaflowRoleType roleType = role.getType();
        if (userRoleMappingDao.existRoleType(tenantId, userId, roleType)) {
            throw new GeaflowIllegalException("User role {} exists", roleType);
        }

        userRoleMappingDao.addRoleType(tenantId, userId, roleType);
    }

    public void deleteRole(String tenantId, String userId, GeaflowRole role) {
        userRoleMappingDao.deleteRoleType(tenantId, userId, role.getType());
    }

    public boolean exist(String userId, GeaflowAuthority authority, GeaflowResource resource) {
        return authorizationDao.exist(userId, authority.getType(), resource.getType(), resource.getId());
    }

    public boolean dropByResources(List<String> resourceIds, GeaflowResourceType type) {
        return authorizationDao.dropByResources(resourceIds, type);
    }

    public void addAuthorization(List<String> resourceIds, String userId, GeaflowAuthorityType authorityType,
                                 GeaflowResourceType resourceType) {
        List<GeaflowAuthorization> authorizations = ListUtil.convert(resourceIds,
            id -> new GeaflowAuthorization(userId, authorityType, resourceType, id));
        create(authorizations);
    }

}

