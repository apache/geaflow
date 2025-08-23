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

package org.apache.geaflow.dsl.common.binary.encoder;

import java.util.List;
import org.apache.geaflow.dsl.common.binary.EncoderFactory;
import org.apache.geaflow.dsl.common.data.RowEdge;
import org.apache.geaflow.dsl.common.data.impl.ObjectRow;
import org.apache.geaflow.dsl.common.data.impl.VertexEdgeFactory;
import org.apache.geaflow.dsl.common.types.EdgeType;
import org.apache.geaflow.dsl.common.types.StructType;
import org.apache.geaflow.dsl.common.types.TableField;
import org.apache.geaflow.dsl.common.util.BinaryUtil;
import org.apache.geaflow.model.graph.IGraphElementWithTimeField;

public class DefaultEdgeEncoder implements EdgeEncoder {

    private final RowEncoder rowEncoder;
    private final EdgeType edgeType;

    public DefaultEdgeEncoder(EdgeType edgeType) {
        StructType rowType = new StructType(edgeType.getValueFields());
        this.rowEncoder = EncoderFactory.createRowEncoder(rowType);
        this.edgeType = edgeType;
    }

    @Override
    public RowEdge encode(RowEdge rowEdge) {
        RowEdge binaryEdge = VertexEdgeFactory.createEdge(edgeType);
        binaryEdge.setSrcId(BinaryUtil.toBinaryForString(rowEdge.getSrcId()));
        binaryEdge.setTargetId(BinaryUtil.toBinaryForString(rowEdge.getTargetId()));
        if (edgeType.getTimestamp().isPresent()) {
            ((IGraphElementWithTimeField) binaryEdge).setTime(((IGraphElementWithTimeField) rowEdge).getTime());
        }
        binaryEdge.setDirect(rowEdge.getDirect());
        binaryEdge.setBinaryLabel(rowEdge.getBinaryLabel());
        Object[] values = new Object[edgeType.getValueSize()];
        List<TableField> valueFields = edgeType.getValueFields();
        for (int i = 0; i < valueFields.size(); i++) {
            values[i] = rowEdge.getField(edgeType.getValueOffset() + i,
                valueFields.get(i).getType());
        }
        binaryEdge.setValue(rowEncoder.encode(ObjectRow.create(values)));
        return binaryEdge;
    }
}
