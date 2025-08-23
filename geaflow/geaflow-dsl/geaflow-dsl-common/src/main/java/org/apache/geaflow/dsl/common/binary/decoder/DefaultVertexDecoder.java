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

package org.apache.geaflow.dsl.common.binary.decoder;

import java.util.List;
import org.apache.geaflow.dsl.common.binary.DecoderFactory;
import org.apache.geaflow.dsl.common.data.RowVertex;
import org.apache.geaflow.dsl.common.data.impl.ObjectRow;
import org.apache.geaflow.dsl.common.data.impl.VertexEdgeFactory;
import org.apache.geaflow.dsl.common.types.StructType;
import org.apache.geaflow.dsl.common.types.TableField;
import org.apache.geaflow.dsl.common.types.VertexType;

public class DefaultVertexDecoder implements VertexDecoder {

    private final RowDecoder rowDecoder;
    private final VertexType vertexType;

    public DefaultVertexDecoder(VertexType vertexType) {
        StructType rowType = new StructType(vertexType.getValueFields());
        this.rowDecoder = DecoderFactory.createRowDecoder(rowType);
        this.vertexType = vertexType;
    }

    @Override
    public RowVertex decode(RowVertex rowVertex) {
        RowVertex decodeVertex = VertexEdgeFactory.createVertex(vertexType);
        decodeVertex.setId(rowVertex.getId());
        decodeVertex.setBinaryLabel(rowVertex.getBinaryLabel());
        Object[] values = new Object[vertexType.getValueSize()];
        List<TableField> valueFields = vertexType.getValueFields();
        for (int i = 0; i < valueFields.size(); i++) {
            values[i] = rowVertex.getField(vertexType.getValueOffset() + i,
                valueFields.get(i).getType());
        }
        decodeVertex.setValue(rowDecoder.decode(ObjectRow.create(values)));
        return decodeVertex;
    }
}
