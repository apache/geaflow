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

import java.util.Optional;
import org.apache.geaflow.console.common.dal.entity.PluginEntity;
import org.apache.geaflow.console.core.model.GeaflowId;
import org.apache.geaflow.console.core.model.file.GeaflowRemoteFile;
import org.apache.geaflow.console.core.model.plugin.GeaflowPlugin;
import org.springframework.stereotype.Component;

@Component
public class PluginConverter extends NameConverter<GeaflowPlugin, PluginEntity> {

    @Override
    protected PluginEntity modelToEntity(GeaflowPlugin model) {
        PluginEntity entity = super.modelToEntity(model);
        entity.setPluginType(model.getType());
        entity.setPluginCategory(model.getCategory());
        entity.setJarPackageId(Optional.ofNullable(model.getJarPackage()).map(GeaflowId::getId).orElse(null));
        return entity;
    }

    @Override
    protected GeaflowPlugin entityToModel(PluginEntity entity) {
        GeaflowPlugin model = super.entityToModel(entity);
        model.setType(entity.getPluginType());
        model.setCategory(entity.getPluginCategory());
        model.setSystem(entity.isSystem());
        return model;
    }

    public GeaflowPlugin convert(PluginEntity entity, GeaflowRemoteFile jarPackage) {
        GeaflowPlugin model = entityToModel(entity);
        model.setJarPackage(jarPackage);
        return model;
    }
}
