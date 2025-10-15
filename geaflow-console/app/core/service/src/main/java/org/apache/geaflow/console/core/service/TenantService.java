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

import java.util.Collection;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.geaflow.console.common.dal.dao.NameDao;
import org.apache.geaflow.console.common.dal.dao.TenantDao;
import org.apache.geaflow.console.common.dal.dao.TenantUserMappingDao;
import org.apache.geaflow.console.common.dal.entity.TenantEntity;
import org.apache.geaflow.console.common.dal.model.PageList;
import org.apache.geaflow.console.common.dal.model.TenantSearch;
import org.apache.geaflow.console.common.util.Fmt;
import org.apache.geaflow.console.common.util.I18nUtil;
import org.apache.geaflow.console.common.util.ListUtil;
import org.apache.geaflow.console.common.util.context.ContextHolder;
import org.apache.geaflow.console.core.model.security.GeaflowTenant;
import org.apache.geaflow.console.core.model.security.GeaflowUser;
import org.apache.geaflow.console.core.service.converter.NameConverter;
import org.apache.geaflow.console.core.service.converter.TenantConverter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class TenantService extends NameService<GeaflowTenant, TenantEntity, TenantSearch> {

    @Autowired
    private TenantDao tenantDao;

    @Autowired
    private TenantUserMappingDao tenantUserMappingDao;

    @Autowired
    private TenantConverter tenantConverter;

    @Override
    protected NameDao<TenantEntity, TenantSearch> getDao() {
        return tenantDao;
    }

    @Override
    protected NameConverter<GeaflowTenant, TenantEntity> getConverter() {
        return tenantConverter;
    }

    @Override
    protected List<GeaflowTenant> parse(List<TenantEntity> entities) {
        return ListUtil.convert(entities, e -> tenantConverter.convert(e));
    }

    @Override
    public PageList<GeaflowTenant> search(TenantSearch search) {
        boolean systemSession = ContextHolder.get().isSystemSession();
        if (systemSession) {
            return super.search(search);
        }

        String userId = ContextHolder.get().getUserId();
        return tenantDao.search(userId, search).transform(this::parse);
    }

    public String getDefaultTenantName(String userName) {
        return "tenant_" + userName;
    }

    public String createDefaultTenant(GeaflowUser user) {
        String userName = user.getName();
        String userComment = user.getComment();
        String tenantName = getDefaultTenantName(userName);
        String userDisplayName = StringUtils.isBlank(userComment) ? userName : userComment;
        String tenantComment = Fmt.as(I18nUtil.getMessage("i18n.key.default.tenant.comment.format"), userDisplayName);

        TenantEntity entity = new TenantEntity();
        entity.setName(tenantName);
        entity.setComment(tenantComment);
        return tenantDao.create(entity);
    }

    public GeaflowTenant getActiveTenant(String userId) {
        TenantEntity entity = tenantUserMappingDao.getUserActiveTenant(userId);
        return parse(entity);
    }

    public List<GeaflowTenant> getUserTenants(String userId) {
        List<TenantEntity> entities = tenantUserMappingDao.getUserTenants(userId);
        return parse(entities);
    }

    @Transactional
    public void activateTenant(String tenantId, String userId) {
        TenantEntity activeTenant = tenantUserMappingDao.getUserActiveTenant(userId);
        if (activeTenant != null && activeTenant.getTenantId().equals(tenantId)) {
            return;
        }

        // reset other tenants
        tenantUserMappingDao.deactivateUserTenants(userId);

        // active current tenant
        tenantUserMappingDao.activateUserTenant(tenantId, userId);
    }

    public Map<String, String> getTenantNames(Collection<String> tenantIds) {
        return tenantDao.getTenantNames(tenantIds);
    }
}
