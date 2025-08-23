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

import java.util.List;
import java.util.stream.Collectors;
import org.apache.geaflow.console.biz.shared.view.FieldView;
import org.apache.geaflow.console.biz.shared.view.StructView;
import org.apache.geaflow.console.core.model.data.GeaflowField;
import org.apache.geaflow.console.core.model.data.GeaflowStruct;
import org.springframework.beans.factory.annotation.Autowired;

public abstract class StructViewConverter<M extends GeaflowStruct, V extends StructView> extends DataViewConverter<M, V> {

    @Autowired
    private FieldViewConverter fieldViewConverter;

    @Override
    protected V modelToView(M model) {
        V view = super.modelToView(model);
        view.setType(model.getType());
        //set field
        List<FieldView> fieldViews = model.getFields().values().stream().map(e -> fieldViewConverter.convert(e))
            .collect(Collectors.toList());
        view.setFields(fieldViews);
        return view;
    }

    @Override
    protected M viewToModel(V view) {
        M model = super.viewToModel(view);
        model.setType(view.getType());
        return model;
    }

    public M convert(V view, List<GeaflowField> fields) {
        M struct = viewToModel(view);
        struct.addFields(fields);
        return struct;
    }
}
