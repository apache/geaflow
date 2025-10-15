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

import org.apache.geaflow.console.biz.shared.view.AuditView;
import org.apache.geaflow.console.core.model.runtime.GeaflowAudit;
import org.springframework.stereotype.Component;

@Component
public class AuditViewConverter extends IdViewConverter<GeaflowAudit, AuditView> {

    @Override
    protected AuditView modelToView(GeaflowAudit model) {
        AuditView view = super.modelToView(model);
        view.setResourceType(model.getResourceType());
        view.setResourceId(model.getResourceId());
        view.setOperationType(model.getOperationType());
        view.setDetail(model.getDetail());
        return view;
    }

    @Override
    protected GeaflowAudit viewToModel(AuditView view) {
        GeaflowAudit model = super.viewToModel(view);
        model.setOperationType(view.getOperationType());
        model.setResourceId(view.getResourceId());
        model.setResourceType(view.getResourceType());
        model.setDetail(view.getDetail());
        return model;
    }

    public GeaflowAudit convert(AuditView view) {
        return viewToModel(view);
    }
}
