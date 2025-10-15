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

import java.util.Optional;
import org.apache.geaflow.console.biz.shared.view.PluginConfigView;
import org.apache.geaflow.console.core.model.plugin.config.GeaflowPluginConfig;
import org.springframework.stereotype.Component;

@Component
public class PluginConfigViewConverter extends NameViewConverter<GeaflowPluginConfig, PluginConfigView> {

    @Override
    public void merge(PluginConfigView view, PluginConfigView updateView) {
        super.merge(view, updateView);
        Optional.ofNullable(updateView.getConfig()).ifPresent(view::setConfig);
        Optional.ofNullable(updateView.getCategory()).ifPresent(view::setCategory);
        Optional.ofNullable(updateView.getType()).ifPresent(view::setType);
    }

    @Override
    protected PluginConfigView modelToView(GeaflowPluginConfig model) {
        PluginConfigView view = super.modelToView(model);
        view.setType(model.getType());
        view.setConfig(model.getConfig());
        view.setCategory(model.getCategory());
        return view;
    }

    @Override
    protected GeaflowPluginConfig viewToModel(PluginConfigView view) {
        GeaflowPluginConfig model = super.viewToModel(view);
        model.setType(view.getType());
        model.setConfig(view.getConfig());
        model.setCategory(view.getCategory());
        return model;
    }

    public GeaflowPluginConfig convert(PluginConfigView view) {
        return viewToModel(view);
    }
}
