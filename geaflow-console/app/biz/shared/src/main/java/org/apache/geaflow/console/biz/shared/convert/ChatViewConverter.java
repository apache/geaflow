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

package org.apache.geaflow.console.biz.shared.convert;

import org.apache.geaflow.console.biz.shared.view.ChatView;
import org.apache.geaflow.console.core.model.llm.GeaflowChat;
import org.springframework.stereotype.Component;

@Component
public class ChatViewConverter extends IdViewConverter<GeaflowChat, ChatView> {

    @Override
    public void merge(ChatView view, ChatView updateView) {
        super.merge(view, updateView);
    }

    @Override
    protected ChatView modelToView(GeaflowChat model) {
        ChatView view = super.modelToView(model);
        view.setModelId(model.getModelId());
        view.setAnswer(model.getAnswer());
        view.setPrompt(model.getPrompt());
        view.setStatus(model.getStatus());
        view.setJobId(model.getJobId());
        return view;
    }

    @Override
    protected GeaflowChat viewToModel(ChatView view) {
        GeaflowChat model = super.viewToModel(view);
        model.setModelId(view.getModelId());
        model.setAnswer(view.getAnswer());
        model.setPrompt(view.getPrompt());
        model.setStatus(view.getStatus());
        model.setJobId(view.getJobId());
        return model;
    }

    public GeaflowChat convert(ChatView view) {
        return viewToModel(view);
    }

}
