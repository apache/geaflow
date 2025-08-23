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
import org.apache.geaflow.console.biz.shared.TenantManager;
import org.apache.geaflow.console.biz.shared.convert.NameViewConverter;
import org.apache.geaflow.console.biz.shared.convert.TenantViewConverter;
import org.apache.geaflow.console.biz.shared.view.TenantView;
import org.apache.geaflow.console.common.dal.entity.TenantEntity;
import org.apache.geaflow.console.common.dal.model.TenantSearch;
import org.apache.geaflow.console.common.util.ListUtil;
import org.apache.geaflow.console.common.util.context.ContextHolder;
import org.apache.geaflow.console.common.util.exception.GeaflowException;
import org.apache.geaflow.console.common.util.exception.GeaflowIllegalException;
import org.apache.geaflow.console.core.model.security.GeaflowTenant;
import org.apache.geaflow.console.core.service.NameService;
import org.apache.geaflow.console.core.service.TenantService;
import org.apache.geaflow.console.core.service.UserService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class TenantManagerImpl extends NameManagerImpl<GeaflowTenant, TenantView, TenantSearch> implements
    TenantManager {

    @Autowired
    private TenantService tenantService;

    @Autowired
    private TenantViewConverter tenantViewConverter;

    @Autowired
    private UserService userService;

    @Override
    protected NameService<GeaflowTenant, TenantEntity, TenantSearch> getService() {
        return tenantService;
    }

    @Override
    protected NameViewConverter<GeaflowTenant, TenantView> getConverter() {
        return tenantViewConverter;
    }

    @Override
    protected List<GeaflowTenant> parse(List<TenantView> views) {
        return ListUtil.convert(views, tenantViewConverter::convert);
    }

    @Override
    public TenantView getActiveTenant(String userId) {
        GeaflowTenant tenant = tenantService.getActiveTenant(userId);
        if (tenant == null) {
            List<GeaflowTenant> userTenants = tenantService.getUserTenants(userId);
            if (userTenants.isEmpty()) {
                throw new GeaflowException("User not in any tenants");
            }

            // active one tenant
            tenant = userTenants.get(0);
            tenantService.activateTenant(userId, tenant.getId());
        }

        return tenantViewConverter.convert(tenant);
    }

    @Override
    public Map<String, String> getTenantNames(Collection<String> tenantIds) {
        return tenantService.getTenantNames(tenantIds);
    }

    @Override
    public TenantView get(String tenantId) {
        boolean systemSession = ContextHolder.get().isSystemSession();
        String userId = ContextHolder.get().getUserId();
        if (!systemSession && !userService.existTenantUser(tenantId, userId)) {
            throw new GeaflowIllegalException("Tenant not found");
        }

        return super.get(tenantId);
    }


}
