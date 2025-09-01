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
import java.util.Optional;
import org.apache.geaflow.console.biz.shared.view.StatementView;
import org.apache.geaflow.console.core.model.statement.GeaflowStatement;
import org.springframework.stereotype.Component;

@Component
public class StatementViewConverter extends IdViewConverter<GeaflowStatement, StatementView> {


    @Override
    protected StatementView modelToView(GeaflowStatement model) {
        StatementView view = super.modelToView(model);
        view.setScript(model.getScript());
        view.setStatus(model.getStatus());
        try {
            view.setResult(JSON.parseObject(model.getResult()));
        } catch (Exception e) {
            view.setResult(model.getResult());
        }

        view.setJobId(model.getJobId());
        return view;
    }

    public GeaflowStatement convert(StatementView view) {
        GeaflowStatement model = super.viewToModel(view);
        model.setScript(view.getScript());
        model.setStatus(view.getStatus());
        model.setResult(Optional.ofNullable(view.getResult()).map(Object::toString).orElse(null));
        model.setJobId(view.getJobId());
        return model;
    }
}
