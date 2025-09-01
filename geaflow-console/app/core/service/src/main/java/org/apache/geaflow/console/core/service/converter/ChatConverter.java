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

import org.apache.geaflow.console.common.dal.entity.ChatEntity;
import org.apache.geaflow.console.core.model.llm.GeaflowChat;
import org.springframework.stereotype.Component;

@Component
public class ChatConverter extends IdConverter<GeaflowChat, ChatEntity> {

    @Override
    protected ChatEntity modelToEntity(GeaflowChat model) {
        ChatEntity entity = super.modelToEntity(model);
        entity.setAnswer(model.getAnswer());
        entity.setPrompt(model.getPrompt());
        entity.setModelId(model.getModelId());
        entity.setStatus(model.getStatus());
        entity.setJobId(model.getJobId());
        return entity;
    }

    @Override
    protected GeaflowChat entityToModel(ChatEntity entity) {
        GeaflowChat model = super.entityToModel(entity);
        model.setAnswer(entity.getAnswer());
        model.setPrompt(entity.getPrompt());
        model.setModelId(entity.getModelId());
        model.setStatus(entity.getStatus());
        model.setJobId(entity.getJobId());
        return model;
    }

    public GeaflowChat convert(ChatEntity entity) {
        return entityToModel(entity);
    }
}
