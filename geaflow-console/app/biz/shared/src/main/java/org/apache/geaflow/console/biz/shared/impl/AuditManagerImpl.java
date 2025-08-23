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

import java.util.List;
import org.apache.geaflow.console.biz.shared.AuditManager;
import org.apache.geaflow.console.biz.shared.convert.AuditViewConverter;
import org.apache.geaflow.console.biz.shared.convert.IdViewConverter;
import org.apache.geaflow.console.biz.shared.view.AuditView;
import org.apache.geaflow.console.common.dal.entity.AuditEntity;
import org.apache.geaflow.console.common.dal.model.AuditSearch;
import org.apache.geaflow.console.common.util.ListUtil;
import org.apache.geaflow.console.core.model.runtime.GeaflowAudit;
import org.apache.geaflow.console.core.service.AuditService;
import org.apache.geaflow.console.core.service.IdService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class AuditManagerImpl extends IdManagerImpl<GeaflowAudit, AuditView, AuditSearch> implements AuditManager {

    @Autowired
    private AuditService auditService;

    @Autowired
    private AuditViewConverter auditViewConverter;

    @Override
    public IdViewConverter<GeaflowAudit, AuditView> getConverter() {
        return auditViewConverter;
    }

    @Override
    public IdService<GeaflowAudit, AuditEntity, AuditSearch> getService() {
        return auditService;
    }

    @Override
    public List<GeaflowAudit> parse(List<AuditView> views) {
        return ListUtil.convert(views, auditViewConverter::convert);
    }

}
