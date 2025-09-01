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

import java.util.Collections;
import java.util.List;
import org.apache.geaflow.console.biz.shared.PluginConfigManager;
import org.apache.geaflow.console.biz.shared.convert.NameViewConverter;
import org.apache.geaflow.console.biz.shared.convert.PluginConfigViewConverter;
import org.apache.geaflow.console.biz.shared.view.PluginConfigView;
import org.apache.geaflow.console.common.dal.entity.PluginConfigEntity;
import org.apache.geaflow.console.common.dal.model.PluginConfigSearch;
import org.apache.geaflow.console.common.util.ListUtil;
import org.apache.geaflow.console.common.util.type.GeaflowPluginCategory;
import org.apache.geaflow.console.core.model.plugin.config.GeaflowPluginConfig;
import org.apache.geaflow.console.core.service.NameService;
import org.apache.geaflow.console.core.service.PluginConfigService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class PluginConfigManagerImpl extends
    NameManagerImpl<GeaflowPluginConfig, PluginConfigView, PluginConfigSearch> implements PluginConfigManager {

    @Autowired
    private PluginConfigService pluginConfigService;

    @Autowired
    private PluginConfigViewConverter pluginConfigViewConverter;

    @Override
    protected NameViewConverter<GeaflowPluginConfig, PluginConfigView> getConverter() {
        return pluginConfigViewConverter;
    }

    @Override
    protected NameService<GeaflowPluginConfig, PluginConfigEntity, PluginConfigSearch> getService() {
        return pluginConfigService;
    }

    @Override
    protected List<GeaflowPluginConfig> parse(List<PluginConfigView> views) {
        return ListUtil.convert(views, v -> pluginConfigViewConverter.convert(v));
    }

    @Override
    public List<PluginConfigView> get(List<String> ids) {
        pluginConfigService.validateGetIds(ids);
        return super.get(ids);
    }

    @Override
    public boolean updateById(String id, PluginConfigView updateView) {
        pluginConfigService.validateUpdateIds(Collections.singletonList(id));
        return super.updateById(id, updateView);
    }

    public boolean drop(List<String> ids) {
        pluginConfigService.validateUpdateIds(ids);
        return super.drop(ids);
    }

    @Override
    public List<PluginConfigView> getPluginConfigs(GeaflowPluginCategory category, String type) {
        return build(pluginConfigService.getPluginConfigs(category, type));
    }
}
