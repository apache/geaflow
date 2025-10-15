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

package org.apache.geaflow.model.graph.meta;

import java.util.ArrayList;
import java.util.List;
import org.apache.geaflow.common.errorcode.RuntimeErrors;
import org.apache.geaflow.common.exception.GeaflowRuntimeException;
import org.apache.geaflow.common.schema.Field;
import org.apache.geaflow.common.schema.ISchema;
import org.apache.geaflow.common.schema.SchemaImpl;
import org.apache.geaflow.common.type.IType;
import org.apache.geaflow.model.graph.IGraphElementWithLabelField;
import org.apache.geaflow.model.graph.IGraphElementWithTimeField;
import org.apache.geaflow.model.graph.edge.IEdge;
import org.apache.geaflow.model.graph.vertex.IVertex;

public class GraphElementSchemaFactory {

    public static final Field FIELD_LABEL = Field.newStringField(GraphFiledName.LABEL.name());
    public static final Field FIELD_TIME = Field.newLongField(GraphFiledName.TIME.name());
    public static final Field FIELD_DIRECTION = Field.newByteField(GraphFiledName.DIRECTION.name());

    public static ISchema newSchema(IType keyType, Class<?> elementClass) {
        if (IVertex.class.isAssignableFrom(elementClass)) {
            return newVertexSchema(keyType, elementClass);
        }
        if (IEdge.class.isAssignableFrom(elementClass)) {
            return newEdgeSchema(keyType, elementClass);
        }
        String msg = "unrecognized graph element class: " + elementClass.getCanonicalName();
        throw new GeaflowRuntimeException(RuntimeErrors.INST.undefinedError(msg));
    }

    private static ISchema newVertexSchema(IType keyType, Class<?> elementClass) {
        List<Field> fields = new ArrayList<>();
        fields.add(new Field(GraphFiledName.ID.name(), keyType));
        if (IGraphElementWithLabelField.class.isAssignableFrom(elementClass)) {
            fields.add(FIELD_LABEL);
        }
        if (IGraphElementWithTimeField.class.isAssignableFrom(elementClass)) {
            fields.add(FIELD_TIME);
        }
        return new SchemaImpl(elementClass.getName(), fields);
    }

    private static ISchema newEdgeSchema(IType keyType, Class<?> elementClass) {
        List<Field> fields = new ArrayList<>();
        fields.add(new Field(GraphFiledName.SRC_ID.name(), keyType));
        fields.add(new Field(GraphFiledName.DST_ID.name(), keyType));
        fields.add(FIELD_DIRECTION);
        if (IGraphElementWithLabelField.class.isAssignableFrom(elementClass)) {
            fields.add(FIELD_LABEL);
        }
        if (IGraphElementWithTimeField.class.isAssignableFrom(elementClass)) {
            fields.add(FIELD_TIME);
        }
        return new SchemaImpl(elementClass.getName(), fields);
    }

}
