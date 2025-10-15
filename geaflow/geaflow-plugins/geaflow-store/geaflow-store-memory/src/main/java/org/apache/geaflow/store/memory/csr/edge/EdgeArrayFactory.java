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

package org.apache.geaflow.store.memory.csr.edge;

import org.apache.geaflow.model.graph.meta.GraphElementMetas.GraphElementFlag;
import org.apache.geaflow.state.schema.GraphDataSchema;
import org.apache.geaflow.store.memory.csr.edge.type.IDEdgeArray;
import org.apache.geaflow.store.memory.csr.edge.type.IDLabelEdgeArray;
import org.apache.geaflow.store.memory.csr.edge.type.IDLabelTimeEdgeArray;
import org.apache.geaflow.store.memory.csr.edge.type.IDTimeEdgeArray;
import org.apache.geaflow.store.memory.csr.edge.type.ValueEdgeArray;
import org.apache.geaflow.store.memory.csr.edge.type.ValueLabelEdgeArray;
import org.apache.geaflow.store.memory.csr.edge.type.ValueLabelTimeEdgeArray;
import org.apache.geaflow.store.memory.csr.edge.type.ValueTimeEdgeArray;

public class EdgeArrayFactory {

    public static IEdgeArray getEdgeArray(GraphDataSchema dataSchema) {
        boolean noProperty = dataSchema.isEmptyEdgeProperty();
        GraphElementFlag flag = GraphElementFlag.build(dataSchema.getEdgeMeta().getGraphElementClass());
        IEdgeArray edgeArray;
        if (flag.isLabeledAndTimed()) {
            edgeArray = noProperty ? new IDLabelTimeEdgeArray<>() : new ValueLabelTimeEdgeArray<>();
        } else if (flag.isLabeled()) {
            edgeArray = noProperty ? new IDLabelEdgeArray<>() : new ValueLabelEdgeArray<>();
        } else if (flag.isTimed()) {
            edgeArray = noProperty ? new IDTimeEdgeArray<>() : new ValueTimeEdgeArray<>();
        } else {
            edgeArray = noProperty ? new IDEdgeArray<>() : new ValueEdgeArray<>();
        }
        return edgeArray;
    }
}
