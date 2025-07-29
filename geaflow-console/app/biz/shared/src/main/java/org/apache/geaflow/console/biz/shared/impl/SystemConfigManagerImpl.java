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

import com.google.common.base.Preconditions;
import java.util.List;
import org.apache.geaflow.console.biz.shared.SystemConfigManager;
import org.apache.geaflow.console.biz.shared.convert.NameViewConverter;
import org.apache.geaflow.console.biz.shared.convert.SystemConfigViewConverter;
import org.apache.geaflow.console.biz.shared.view.SystemConfigView;
import org.apache.geaflow.console.common.dal.model.SystemConfigSearch;
import org.apache.geaflow.console.common.util.ListUtil;
import org.apache.geaflow.console.common.util.exception.GeaflowIllegalException;
import org.apache.geaflow.console.core.model.config.GeaflowSystemConfig;
import org.apache.geaflow.console.core.service.NameService;
import org.apache.geaflow.console.core.service.SystemConfigService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class SystemConfigManagerImpl extends
    NameManagerImpl<GeaflowSystemConfig, SystemConfigView, SystemConfigSearch> implements SystemConfigManager {

    @Autowired
    private SystemConfigService systemConfigService;

    @Autowired
    private SystemConfigViewConverter systemConfigViewConverter;

    @Override
    protected NameService<GeaflowSystemConfig, ?, SystemConfigSearch> getService() {
        return systemConfigService;
    }

    @Override
    protected NameViewConverter<GeaflowSystemConfig, SystemConfigView> getConverter() {
        return systemConfigViewConverter;
    }

    @Override
    protected List<GeaflowSystemConfig> parse(List<SystemConfigView> views) {
        return ListUtil.convert(views, systemConfigViewConverter::convert);
    }

    @Override
    public SystemConfigView getConfig(String tenantId, String key) {
        Preconditions.checkNotNull(key, "Invalid key");
        return build(systemConfigService.get(tenantId, key));
    }

    @Override
    public String getValue(String key) {
        return systemConfigService.getValue(key);
    }

    @Override
    public boolean createConfig(SystemConfigView view) {
        String tenantId = view.getTenantId();
        String key = Preconditions.checkNotNull(view.getName(), "Invalid key");
        if (systemConfigService.exist(tenantId, key)) {
            throw new GeaflowIllegalException("Key {} exists", key);
        }

        return create(view) != null;
    }

    @Override
    public boolean updateConfig(String key, SystemConfigView updateView) {
        String newKey = updateView.getName();
        if (newKey != null && !newKey.equals(key)) {
            throw new GeaflowIllegalException("Rename key from {} to {} not allowed", key, newKey);
        }

        String tenantId = updateView.getTenantId();
        if (!systemConfigService.exist(tenantId, key)) {
            throw new GeaflowIllegalException("Key {} not exists", key);
        }

        SystemConfigView view = getConfig(tenantId, key);
        return updateById(view.getId(), updateView);
    }

    @Override
    public boolean deleteConfig(String tenantId, String key) {
        Preconditions.checkNotNull(key, "Invalid key");
        return systemConfigService.delete(tenantId, key);
    }
}
