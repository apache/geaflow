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
import org.apache.geaflow.console.biz.shared.view.ClusterView;
import org.apache.geaflow.console.core.model.cluster.GeaflowCluster;
import org.springframework.stereotype.Component;

@Component
public class ClusterViewConverter extends NameViewConverter<GeaflowCluster, ClusterView> {

    @Override
    public void merge(ClusterView view, ClusterView updateView) {
        super.merge(view, updateView);
        Optional.ofNullable(updateView.getType()).ifPresent(view::setType);
        Optional.ofNullable(updateView.getConfig()).ifPresent(view::setConfig);
    }

    @Override
    protected ClusterView modelToView(GeaflowCluster model) {
        ClusterView view = super.modelToView(model);
        view.setType(model.getType());
        view.setConfig(model.getConfig());
        return view;
    }

    @Override
    protected GeaflowCluster viewToModel(ClusterView view) {
        GeaflowCluster model = super.viewToModel(view);
        model.setType(view.getType());
        model.setConfig(view.getConfig());
        return model;
    }

    public GeaflowCluster convert(ClusterView view) {
        return viewToModel(view);
    }

}
