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

package org.apache.geaflow.store.memory.csr.vertex;

import org.apache.geaflow.model.graph.meta.GraphElementMetas.GraphElementFlag;
import org.apache.geaflow.state.schema.GraphDataSchema;
import org.apache.geaflow.store.memory.csr.vertex.type.IDLabelTimeVertexArray;
import org.apache.geaflow.store.memory.csr.vertex.type.IDLabelVertexArray;
import org.apache.geaflow.store.memory.csr.vertex.type.IDTimeVertexArray;
import org.apache.geaflow.store.memory.csr.vertex.type.IDVertexArray;
import org.apache.geaflow.store.memory.csr.vertex.type.ValueLabelTimeVertexArray;
import org.apache.geaflow.store.memory.csr.vertex.type.ValueLabelVertexArray;
import org.apache.geaflow.store.memory.csr.vertex.type.ValueTimeVertexArray;
import org.apache.geaflow.store.memory.csr.vertex.type.ValueVertexArray;

public class VertexArrayFactory {

    public static IVertexArray getVertexArray(GraphDataSchema dataSchema) {
        boolean noProperty = dataSchema.isEmptyVertexProperty();
        GraphElementFlag flag = GraphElementFlag.build(dataSchema.getVertexMeta().getGraphElementClass());
        IVertexArray vertexArray;
        if (flag.isLabeledAndTimed()) {
            vertexArray = noProperty ? new IDLabelTimeVertexArray<>() : new ValueLabelTimeVertexArray<>();
        } else if (flag.isLabeled()) {
            vertexArray = noProperty ? new IDLabelVertexArray<>() : new ValueLabelVertexArray<>();
        } else if (flag.isTimed()) {
            vertexArray = noProperty ? new IDTimeVertexArray<>() : new ValueTimeVertexArray<>();
        } else {
            vertexArray = noProperty ? new IDVertexArray<>() : new ValueVertexArray<>();
        }
        return vertexArray;
    }

}
