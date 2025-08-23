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

import org.apache.geaflow.console.common.dal.entity.RemoteFileEntity;
import org.apache.geaflow.console.core.model.file.GeaflowRemoteFile;
import org.springframework.stereotype.Component;

@Component
public class RemoteFileConverter extends NameConverter<GeaflowRemoteFile, RemoteFileEntity> {

    @Override
    protected RemoteFileEntity modelToEntity(GeaflowRemoteFile model) {
        RemoteFileEntity entity = super.modelToEntity(model);
        entity.setMd5(model.getMd5());
        entity.setType(model.getType());
        entity.setPath(model.getPath());
        entity.setUrl(model.getUrl());
        return entity;
    }

    @Override
    protected GeaflowRemoteFile entityToModel(RemoteFileEntity entity) {
        GeaflowRemoteFile model = super.entityToModel(entity);
        model.setPath(entity.getPath());
        model.setMd5(entity.getMd5());
        model.setUrl(entity.getUrl());
        model.setType(entity.getType());
        return model;
    }

    public GeaflowRemoteFile convert(RemoteFileEntity entity) {
        return entityToModel(entity);
    }
}
