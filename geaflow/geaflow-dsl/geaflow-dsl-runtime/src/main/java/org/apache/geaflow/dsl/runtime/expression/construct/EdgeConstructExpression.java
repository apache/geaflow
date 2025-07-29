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

package org.apache.geaflow.dsl.runtime.expression.construct;

import java.util.List;
import org.apache.geaflow.common.binary.BinaryString;
import org.apache.geaflow.common.type.IType;
import org.apache.geaflow.dsl.common.data.Row;
import org.apache.geaflow.dsl.common.data.RowEdge;
import org.apache.geaflow.dsl.common.data.impl.ObjectRow;
import org.apache.geaflow.dsl.common.data.impl.VertexEdgeFactory;
import org.apache.geaflow.dsl.common.types.EdgeType;
import org.apache.geaflow.dsl.runtime.expression.AbstractNonLeafExpression;
import org.apache.geaflow.dsl.runtime.expression.Expression;
import org.apache.geaflow.model.graph.IGraphElementWithTimeField;

public class EdgeConstructExpression extends AbstractNonLeafExpression {

    private final Expression srcIdExpression;

    private final Expression targetIdExpression;

    private final Expression labelExpression;

    private final Expression tsExpression;

    private final EdgeType edgeType;

    public EdgeConstructExpression(List<Expression> inputs, IType<?> outputType) {
        super(inputs, outputType);
        this.srcIdExpression = inputs.get(EdgeType.SRC_ID_FIELD_POSITION);
        this.targetIdExpression = inputs.get(EdgeType.TARGET_ID_FIELD_POSITION);
        this.labelExpression = inputs.get(EdgeType.LABEL_FIELD_POSITION);
        this.edgeType = (EdgeType) outputType;
        if (this.edgeType.getTimestamp().isPresent()) {
            tsExpression = inputs.get(EdgeType.TIME_FIELD_POSITION);
        } else {
            tsExpression = null;
        }
    }

    @Override
    public Object evaluate(Row row) {
        Object srcId = srcIdExpression.evaluate(row);
        Object targetId = targetIdExpression.evaluate(row);

        Object[] values = new Object[edgeType.getValueSize()];
        for (int i = edgeType.getValueOffset(); i < edgeType.size(); i++) {
            values[i - edgeType.getValueOffset()] = inputs.get(i).evaluate(row);
        }
        RowEdge edge = VertexEdgeFactory.createEdge((EdgeType) outputType);
        BinaryString label = (BinaryString) labelExpression.evaluate(row);
        edge.setSrcId(srcId);
        edge.setTargetId(targetId);
        edge.setBinaryLabel(label);
        edge.setValue(ObjectRow.create(values));

        if (tsExpression != null) {
            Long ts = (Long) tsExpression.evaluate(row);
            assert ts != null;
            ((IGraphElementWithTimeField) edge).setTime(ts);
        }
        return edge;
    }

    @Override
    public String showExpression() {
        StringBuilder str = new StringBuilder();
        for (int i = 0; i < edgeType.size(); i++) {
            if (i > 0) {
                str.append(",");
            }
            str.append(edgeType.getField(i).getName()).append(":").append(inputs.get(i).showExpression());
        }
        return "Edge{" + str + "}";
    }

    @Override
    public Expression copy(List<Expression> inputs) {
        return new EdgeConstructExpression(inputs, getOutputType());
    }
}
