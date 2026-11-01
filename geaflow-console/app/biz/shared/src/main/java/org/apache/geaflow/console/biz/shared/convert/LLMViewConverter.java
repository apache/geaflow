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

import com.alibaba.fastjson.JSON;
import java.util.Objects;
import java.util.Optional;
import org.apache.geaflow.console.biz.shared.view.LLMView;
import org.apache.geaflow.console.common.util.exception.GeaflowIllegalException;
import org.apache.geaflow.console.core.model.config.ConfigRedactor;
import org.apache.geaflow.console.core.model.config.GeaflowConfig;
import org.apache.geaflow.console.core.model.llm.GeaflowLLM;
import org.springframework.stereotype.Component;

@Component
public class LLMViewConverter extends NameViewConverter<GeaflowLLM, LLMView> {

    @Override
    public void merge(LLMView view, LLMView updateView) {
        super.merge(view, updateView);
        boolean sameProvider = (updateView.getType() == null || Objects.equals(view.getType(), updateView.getType()))
            && (updateView.getUrl() == null || Objects.equals(view.getUrl(), updateView.getUrl()));
        Optional.ofNullable(updateView.getType()).ifPresent(view::setType);
        Optional.ofNullable(updateView.getUrl()).ifPresent(view::setUrl);
        if (updateView.getArgs() != null) {
            GeaflowConfig previous = sameProvider ? JSON.parseObject(view.getArgs(), GeaflowConfig.class) : null;
            GeaflowConfig update = JSON.parseObject(updateView.getArgs(), GeaflowConfig.class);
            view.setArgs(JSON.toJSONString(ConfigRedactor.merge(update, previous, ConfigRedactor.llmDesc(view.getType()))));
        } else if (!sameProvider) {
            throw new GeaflowIllegalException(
                "Changing LLM type or URL requires new arguments");
        }
    }

    @Override
    protected LLMView modelToView(GeaflowLLM model) {
        LLMView view = super.modelToView(model);
        view.setType(model.getType());
        view.setUrl(model.getUrl());
        view.setArgs(JSON.toJSONString(model.getArgs()));
        return view;
    }

    @Override
    protected GeaflowLLM viewToModel(LLMView view) {
        GeaflowLLM model = super.viewToModel(view);
        model.setType(view.getType());
        model.setUrl(view.getUrl());

        GeaflowConfig config = Optional.ofNullable(JSON.parseObject(view.getArgs(), GeaflowConfig.class)).orElse(new GeaflowConfig());
        ConfigRedactor.rejectUnresolved(config);
        model.setArgs(config);
        return model;
    }

    public GeaflowLLM convert(LLMView view) {
        return viewToModel(view);
    }

}
