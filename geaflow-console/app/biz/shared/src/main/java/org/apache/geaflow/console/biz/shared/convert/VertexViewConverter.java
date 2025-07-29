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
import java.util.Optional;
import org.apache.geaflow.console.biz.shared.view.VertexView;
import org.apache.geaflow.console.common.util.type.GeaflowStructType;
import org.apache.geaflow.console.core.model.data.GeaflowField;
import org.apache.geaflow.console.core.model.data.GeaflowVertex;
import org.springframework.stereotype.Component;

@Component
public class VertexViewConverter extends StructViewConverter<GeaflowVertex, VertexView> {

    @Override
    protected VertexView modelToView(GeaflowVertex model) {
        VertexView vertexView = super.modelToView(model);
        vertexView.setType(GeaflowStructType.VERTEX);
        return vertexView;
    }

    @Override
    public void merge(VertexView view, VertexView updateView) {
        super.merge(view, updateView);
        Optional.ofNullable(updateView.getFields()).ifPresent(view::setFields);
    }

    @Override
    protected GeaflowVertex viewToModel(VertexView view) {
        GeaflowVertex vertex = super.viewToModel(view);
        vertex.setType(GeaflowStructType.VERTEX);
        return vertex;
    }

    public GeaflowVertex converter(VertexView view, List<GeaflowField> fields) {
        GeaflowVertex vertex = viewToModel(view);
        vertex.addFields(fields);
        return vertex;
    }
}
