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

import org.apache.geaflow.console.common.dal.entity.AuthorizationEntity;
import org.apache.geaflow.console.core.model.security.GeaflowAuthorization;
import org.springframework.stereotype.Component;

@Component
public class AuthorizationConverter extends IdConverter<GeaflowAuthorization, AuthorizationEntity> {

    @Override
    protected AuthorizationEntity modelToEntity(GeaflowAuthorization model) {
        AuthorizationEntity entity = super.modelToEntity(model);
        entity.setResourceType(model.getResourceType());
        entity.setAuthorityType(model.getAuthorityType());
        entity.setUserId(model.getUserId());
        entity.setResourceId(model.getResourceId());
        return entity;
    }

    @Override
    protected GeaflowAuthorization entityToModel(AuthorizationEntity entity) {
        GeaflowAuthorization model = super.entityToModel(entity);
        model.setResourceType(entity.getResourceType());
        model.setAuthorityType(entity.getAuthorityType());
        model.setUserId(entity.getUserId());
        model.setResourceId(entity.getResourceId());
        return model;
    }

    public GeaflowAuthorization convert(AuthorizationEntity entity) {
        return entityToModel(entity);
    }

}
