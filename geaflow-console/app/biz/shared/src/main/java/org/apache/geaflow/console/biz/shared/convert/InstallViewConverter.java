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

package org.apache.geaflow.console.biz.shared.convert;

import org.apache.geaflow.console.biz.shared.view.InstallView;
import org.apache.geaflow.console.core.model.install.GeaflowInstall;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class InstallViewConverter extends IdViewConverter<GeaflowInstall, InstallView> {

    @Autowired
    private PluginConfigViewConverter pluginConfigViewConverter;

    @Override
    protected InstallView modelToView(GeaflowInstall model) {
        InstallView view = super.modelToView(model);
        view.setRuntimeClusterConfig(pluginConfigViewConverter.convert(model.getRuntimeClusterConfig()));
        view.setRuntimeMetaConfig(pluginConfigViewConverter.convert(model.getRuntimeMetaConfig()));
        view.setHaMetaConfig(pluginConfigViewConverter.convert(model.getHaMetaConfig()));
        view.setMetricConfig(pluginConfigViewConverter.convert(model.getMetricConfig()));
        view.setRemoteFileConfig(pluginConfigViewConverter.convert(model.getRemoteFileConfig()));
        view.setDataConfig(pluginConfigViewConverter.convert(model.getDataConfig()));
        return view;
    }

    @Override
    protected GeaflowInstall viewToModel(InstallView view) {
        GeaflowInstall model = super.viewToModel(view);
        model.setRuntimeClusterConfig(pluginConfigViewConverter.convert(view.getRuntimeClusterConfig()));
        model.setRuntimeMetaConfig(pluginConfigViewConverter.convert(view.getRuntimeMetaConfig()));
        model.setHaMetaConfig(pluginConfigViewConverter.convert(view.getHaMetaConfig()));
        model.setMetricConfig(pluginConfigViewConverter.convert(view.getMetricConfig()));
        model.setRemoteFileConfig(pluginConfigViewConverter.convert(view.getRemoteFileConfig()));
        model.setDataConfig(pluginConfigViewConverter.convert(view.getDataConfig()));
        return model;
    }

    public GeaflowInstall convert(InstallView view) {
        return viewToModel(view);
    }

}
