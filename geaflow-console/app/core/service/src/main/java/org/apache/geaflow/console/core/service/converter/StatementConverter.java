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

package org.apache.geaflow.console.core.service.converter;

import org.apache.geaflow.console.common.dal.entity.StatementEntity;
import org.apache.geaflow.console.core.model.statement.GeaflowStatement;
import org.springframework.stereotype.Component;

@Component
public class StatementConverter extends IdConverter<GeaflowStatement, StatementEntity> {

    @Override
    protected StatementEntity modelToEntity(GeaflowStatement model) {
        StatementEntity entity = super.modelToEntity(model);
        entity.setScript(model.getScript());
        entity.setStatus(model.getStatus());
        entity.setResult(model.getResult());
        entity.setJobId(model.getJobId());
        return entity;
    }


    public GeaflowStatement convert(StatementEntity entity) {
        GeaflowStatement model = super.entityToModel(entity);
        model.setScript(entity.getScript());
        model.setStatus(entity.getStatus());
        model.setResult(entity.getResult());
        model.setJobId(entity.getJobId());
        return model;
    }
}
