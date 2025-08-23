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

import java.util.List;
import org.apache.geaflow.console.common.dal.entity.VertexEntity;
import org.apache.geaflow.console.core.model.data.GeaflowField;
import org.apache.geaflow.console.core.model.data.GeaflowVertex;
import org.springframework.stereotype.Component;

@Component
public class VertexConverter extends DataConverter<GeaflowVertex, VertexEntity> {

    public GeaflowVertex convert(VertexEntity entity, List<GeaflowField> fields) {
        GeaflowVertex vertex = entityToModel(entity);
        vertex.addFields(fields);
        return vertex;
    }
}
