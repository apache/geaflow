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

import org.apache.geaflow.console.common.dal.entity.NameEntity;
import org.apache.geaflow.console.core.model.GeaflowName;

public abstract class NameConverter<M extends GeaflowName, E extends NameEntity> extends IdConverter<M, E> {

    protected E modelToEntity(M model) {
        E entity = super.modelToEntity(model);
        entity.setName(model.getName());
        entity.setComment(model.getComment());
        return entity;
    }

    @Override
    protected M entityToModel(E entity) {
        M model = super.entityToModel(entity);
        model.setName(entity.getName());
        model.setComment(entity.getComment());
        return model;
    }

    @Override
    protected M entityToModel(E entity, Class<? extends M> clazz) {
        M model = super.entityToModel(entity, clazz);
        model.setName(entity.getName());
        model.setComment(entity.getComment());
        return model;
    }
}
