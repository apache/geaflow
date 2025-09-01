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
import org.apache.geaflow.console.biz.shared.view.SystemConfigView;
import org.apache.geaflow.console.core.model.config.GeaflowSystemConfig;
import org.springframework.stereotype.Component;

@Component
public class SystemConfigViewConverter extends NameViewConverter<GeaflowSystemConfig, SystemConfigView> {

    @Override
    public void merge(SystemConfigView view, SystemConfigView updateView) {
        super.merge(view, updateView);
        Optional.ofNullable(updateView.getValue()).ifPresent(view::setValue);
    }

    @Override
    protected SystemConfigView modelToView(GeaflowSystemConfig model) {
        SystemConfigView view = super.modelToView(model);
        view.setValue(model.getValue());
        return view;
    }

    @Override
    protected GeaflowSystemConfig viewToModel(SystemConfigView view) {
        GeaflowSystemConfig model = super.viewToModel(view);
        model.setTenantId(view.getTenantId());
        model.setValue(view.getValue());
        return model;
    }

    public GeaflowSystemConfig convert(SystemConfigView view) {
        return viewToModel(view);
    }
}
