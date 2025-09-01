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
import org.apache.geaflow.console.biz.shared.SystemConfigManager;
import org.apache.geaflow.console.biz.shared.view.SystemConfigView;
import org.apache.geaflow.console.common.dal.model.PageList;
import org.apache.geaflow.console.common.dal.model.SystemConfigSearch;
import org.apache.geaflow.console.core.model.security.GeaflowRole;
import org.apache.geaflow.console.web.api.GeaflowApiResponse;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;


@RestController
@RequestMapping("/configs")
public class SystemConfigController {

    @Autowired
    private SystemConfigManager systemConfigManager;

    @Autowired
    private AuthorizationManager authorizationManager;

    @GetMapping
    public GeaflowApiResponse<PageList<SystemConfigView>> searchConfigs(SystemConfigSearch search) {
        authorizationManager.hasRole(GeaflowRole.SYSTEM_ADMIN);
        return GeaflowApiResponse.success(systemConfigManager.search(search));
    }

    @GetMapping("/{key}")
    public GeaflowApiResponse<SystemConfigView> getConfig(@PathVariable String key,
                                                          @RequestParam(required = false) String tenantId) {
        authorizationManager.hasRole(GeaflowRole.SYSTEM_ADMIN);
        return GeaflowApiResponse.success(systemConfigManager.getConfig(tenantId, key));
    }

    @GetMapping("/{key}/value")
    public GeaflowApiResponse<String> getValue(@PathVariable String key) {
        return GeaflowApiResponse.success(systemConfigManager.getValue(key));
    }

    @PostMapping
    public GeaflowApiResponse<Boolean> createConfig(@RequestBody SystemConfigView view) {
        authorizationManager.hasRole(GeaflowRole.SYSTEM_ADMIN);
        return GeaflowApiResponse.success(systemConfigManager.createConfig(view));
    }

    @PutMapping("/{key}")
    public GeaflowApiResponse<Boolean> updateConfig(@PathVariable String key, @RequestBody SystemConfigView view) {
        authorizationManager.hasRole(GeaflowRole.SYSTEM_ADMIN);
        return GeaflowApiResponse.success(systemConfigManager.updateConfig(key, view));
    }

    @DeleteMapping("/{key}")
    public GeaflowApiResponse<Boolean> deleteConfig(@PathVariable String key,
                                                    @RequestParam(required = false) String tenantId) {
        authorizationManager.hasRole(GeaflowRole.SYSTEM_ADMIN);
        return GeaflowApiResponse.success(systemConfigManager.deleteConfig(tenantId, key));
    }

}
