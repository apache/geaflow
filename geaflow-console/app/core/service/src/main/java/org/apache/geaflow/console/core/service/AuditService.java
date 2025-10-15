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

package org.apache.geaflow.console.core.service;

import java.util.List;
import org.apache.geaflow.console.common.dal.dao.AuditDao;
import org.apache.geaflow.console.common.dal.dao.IdDao;
import org.apache.geaflow.console.common.dal.entity.AuditEntity;
import org.apache.geaflow.console.common.dal.model.AuditSearch;
import org.apache.geaflow.console.common.util.ListUtil;
import org.apache.geaflow.console.core.model.runtime.GeaflowAudit;
import org.apache.geaflow.console.core.service.converter.AuditConverter;
import org.apache.geaflow.console.core.service.converter.IdConverter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class AuditService extends IdService<GeaflowAudit, AuditEntity, AuditSearch> {

    @Autowired
    private AuditDao auditDao;

    @Autowired
    private AuditConverter auditConverter;

    @Override
    protected IdDao<AuditEntity, AuditSearch> getDao() {
        return auditDao;
    }

    @Override
    protected IdConverter<GeaflowAudit, AuditEntity> getConverter() {
        return auditConverter;
    }

    @Override
    protected List<GeaflowAudit> parse(List<AuditEntity> auditEntities) {
        return ListUtil.convert(auditEntities, e -> auditConverter.convert(e));
    }
}

