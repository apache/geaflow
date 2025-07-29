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

import org.apache.geaflow.console.common.dal.entity.FieldEntity;
import org.apache.geaflow.console.common.util.type.GeaflowResourceType;
import org.apache.geaflow.console.core.model.data.GeaflowField;
import org.springframework.stereotype.Component;

@Component
public class FieldConverter extends NameConverter<GeaflowField, FieldEntity> {

    @Override
    protected FieldEntity modelToEntity(GeaflowField model) {
        FieldEntity entity = super.modelToEntity(model);
        entity.setType(model.getType());
        entity.setCategory(model.getCategory());
        return entity;
    }

    @Override
    protected GeaflowField entityToModel(FieldEntity entity) {
        GeaflowField model = super.entityToModel(entity);
        model.setType(entity.getType());
        model.setCategory(entity.getCategory());
        return model;
    }

    public GeaflowField convert(FieldEntity entity) {
        return entityToModel(entity);
    }


    public FieldEntity convert(GeaflowField field, String resourceId, GeaflowResourceType resourceType, int index) {
        FieldEntity entity = modelToEntity(field);
        entity.setResourceId(resourceId);
        entity.setResourceType(resourceType);
        entity.setSortKey(index);
        return entity;
    }

}
