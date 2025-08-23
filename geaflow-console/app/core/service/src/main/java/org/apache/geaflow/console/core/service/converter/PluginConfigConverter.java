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

import com.alibaba.fastjson.JSON;
import org.apache.geaflow.console.common.dal.entity.PluginConfigEntity;
import org.apache.geaflow.console.common.util.type.GeaflowPluginType;
import org.apache.geaflow.console.core.model.config.GeaflowConfig;
import org.apache.geaflow.console.core.model.plugin.config.GeaflowPluginConfig;
import org.springframework.stereotype.Component;

@Component
public class PluginConfigConverter extends NameConverter<GeaflowPluginConfig, PluginConfigEntity> {

    @Override
    protected PluginConfigEntity modelToEntity(GeaflowPluginConfig model) {
        PluginConfigEntity entity = super.modelToEntity(model);
        entity.setType(model.getType());
        entity.setConfig(JSON.toJSONString(model.getConfig()));
        entity.setCategory(model.getCategory());
        return entity;
    }

    @Override
    protected GeaflowPluginConfig entityToModel(PluginConfigEntity entity) {
        GeaflowPluginConfig model = super.entityToModel(entity);
        model.setType(GeaflowPluginType.getName(entity.getType()));
        model.setConfig(JSON.parseObject(entity.getConfig(), GeaflowConfig.class));
        model.setCategory(entity.getCategory());
        return model;
    }

    public GeaflowPluginConfig convert(PluginConfigEntity entity) {
        return entityToModel(entity);
    }
}
