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

import org.apache.geaflow.console.common.dal.entity.FunctionEntity;
import org.apache.geaflow.console.core.model.data.GeaflowFunction;
import org.apache.geaflow.console.core.model.file.GeaflowRemoteFile;
import org.springframework.stereotype.Component;

@Component
public class FunctionConverter extends DataConverter<GeaflowFunction, FunctionEntity> {

    @Override
    protected FunctionEntity modelToEntity(GeaflowFunction model) {
        FunctionEntity entity = super.modelToEntity(model);
        entity.setJarPackageId(model.getJarPackage().getId());
        entity.setEntryClass(model.getEntryClass());
        return entity;
    }

    @Override
    protected GeaflowFunction entityToModel(FunctionEntity entity) {
        GeaflowFunction model = super.entityToModel(entity);
        model.setEntryClass(entity.getEntryClass());
        return model;
    }

    public GeaflowFunction convert(FunctionEntity entity, GeaflowRemoteFile jarPackage) {
        GeaflowFunction model = entityToModel(entity);
        model.setJarPackage(jarPackage);
        return model;
    }
}
