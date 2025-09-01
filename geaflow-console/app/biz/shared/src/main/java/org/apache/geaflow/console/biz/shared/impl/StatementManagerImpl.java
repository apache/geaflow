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
import lombok.extern.slf4j.Slf4j;
import org.apache.geaflow.console.biz.shared.StatementManager;
import org.apache.geaflow.console.biz.shared.convert.IdViewConverter;
import org.apache.geaflow.console.biz.shared.convert.StatementViewConverter;
import org.apache.geaflow.console.biz.shared.view.StatementView;
import org.apache.geaflow.console.common.dal.entity.StatementEntity;
import org.apache.geaflow.console.common.dal.model.StatementSearch;
import org.apache.geaflow.console.common.util.ListUtil;
import org.apache.geaflow.console.core.model.statement.GeaflowStatement;
import org.apache.geaflow.console.core.service.IdService;
import org.apache.geaflow.console.core.service.StatementService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class StatementManagerImpl extends IdManagerImpl<GeaflowStatement, StatementView, StatementSearch>
    implements StatementManager {

    @Autowired
    private StatementService statementService;

    @Autowired
    private StatementViewConverter statementViewConverter;

    @Override
    public IdViewConverter<GeaflowStatement, StatementView> getConverter() {
        return statementViewConverter;
    }

    @Override
    protected List<GeaflowStatement> parse(List<StatementView> views) {
        return ListUtil.convert(views, v -> statementViewConverter.convert(v));
    }

    @Override
    public IdService<GeaflowStatement, StatementEntity, StatementSearch> getService() {
        return statementService;
    }

    @Override
    public boolean dropByJobId(String jobId) {
        return statementService.dropByJobIds(Collections.singletonList(jobId));
    }
}
