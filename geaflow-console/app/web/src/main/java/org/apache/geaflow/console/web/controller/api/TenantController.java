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

package org.apache.geaflow.console.web.controller.api;

import org.apache.geaflow.console.biz.shared.AuthorizationManager;
import org.apache.geaflow.console.biz.shared.TenantManager;
import org.apache.geaflow.console.biz.shared.view.TenantView;
import org.apache.geaflow.console.common.dal.model.PageList;
import org.apache.geaflow.console.common.dal.model.TenantSearch;
import org.apache.geaflow.console.common.util.exception.GeaflowException;
import org.apache.geaflow.console.core.model.security.GeaflowAuthority;
import org.apache.geaflow.console.core.model.security.GeaflowRole;
import org.apache.geaflow.console.core.service.security.Resources;
import org.apache.geaflow.console.web.api.GeaflowApiResponse;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;


@RestController
@RequestMapping("/tenants")
public class TenantController {

    @Autowired
    private TenantManager tenantManager;

    @Autowired
    private AuthorizationManager authorizationManager;

    @GetMapping
    public GeaflowApiResponse<PageList<TenantView>> searchTenants(TenantSearch search) {
        return GeaflowApiResponse.success(tenantManager.search(search));
    }

    @GetMapping("/{tenantId}")
    public GeaflowApiResponse<TenantView> getTenant(@PathVariable String tenantId) {
        return GeaflowApiResponse.success(tenantManager.get(tenantId));
    }

    @PostMapping
    public GeaflowApiResponse<String> createTenant(@RequestBody TenantView tenantView) {
        authorizationManager.hasRole(GeaflowRole.SYSTEM_ADMIN);
        return GeaflowApiResponse.success(tenantManager.create(tenantView));
    }

    @PutMapping("/{tenantId}")
    public GeaflowApiResponse<Boolean> updateTenant(@PathVariable String tenantId, @RequestBody TenantView tenantView) {
        authorizationManager.hasAuthority(GeaflowAuthority.UPDATE, Resources.tenant(tenantId));
        return GeaflowApiResponse.success(tenantManager.updateById(tenantId, tenantView));
    }

    @DeleteMapping("/{tenantId}")
    public GeaflowApiResponse<Boolean> deleteTenant(@PathVariable String tenantId) {
        throw new GeaflowException("Delete tenant {} not allowed", tenantId);
    }
}
