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

package org.apache.geaflow.dsl.runtime.function.graph;

import com.google.common.collect.ImmutableList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.geaflow.common.type.IType;
import org.apache.geaflow.dsl.common.data.Path;
import org.apache.geaflow.dsl.common.data.Row;
import org.apache.geaflow.dsl.common.data.RowVertex;
import org.apache.geaflow.dsl.common.data.StepRecord;
import org.apache.geaflow.dsl.common.data.impl.DefaultPath;
import org.apache.geaflow.dsl.common.types.VertexType;
import org.apache.geaflow.dsl.runtime.expression.Expression;
import org.apache.geaflow.dsl.runtime.expression.construct.VertexConstructExpression;
import org.apache.geaflow.dsl.runtime.traversal.TraversalRuntimeContext;
import org.apache.geaflow.dsl.runtime.traversal.collector.StepCollector;
import org.apache.geaflow.dsl.runtime.traversal.data.GlobalVariable;

public class StepPathModifyFunction implements StepMapFunction {

    protected final int[] updatePathIndices;

    protected final Expression[] modifyExpressions;

    protected final IType<?>[] fieldTypes;

    protected TraversalRuntimeContext context;

    private FunctionSchemas schemas;

    private final int newFieldNum;

    public StepPathModifyFunction(int[] updatePathIndices,
                                  Expression[] modifyExpressions,
                                  IType<?>[] fieldTypes) {
        this.updatePathIndices = Objects.requireNonNull(updatePathIndices);
        this.modifyExpressions = Objects.requireNonNull(modifyExpressions);
        assert updatePathIndices.length == modifyExpressions.length;
        this.fieldTypes = Objects.requireNonNull(fieldTypes);
        this.newFieldNum = this.updatePathIndices.length > 0 ? Math.max(fieldTypes.length,
            1 + Arrays.stream(updatePathIndices).max().getAsInt()) : fieldTypes.length;
    }

    @Override
    public void open(TraversalRuntimeContext context, FunctionSchemas schemas) {
        this.context = context;
        for (Expression expression : modifyExpressions) {
            StepFunction.openExpression(expression, context);
        }
        this.schemas = schemas;
        for (int i = 0; i < updatePathIndices.length; i++) {
            if (modifyExpressions[i] instanceof VertexConstructExpression) {
                VertexConstructExpression vertexConstruct = (VertexConstructExpression) modifyExpressions[i];
                List<GlobalVariable> globalVariables = vertexConstruct.getGlobalVariables();
                for (GlobalVariable gv : globalVariables) {
                    gv.setAddFieldIndex(ArrayUtils.indexOf(schemas.getAddingVertexFieldNames(), gv.getName()));
                }
            }
        }
    }

    @Override
    public void finish(StepCollector<StepRecord> collector) {

    }

    @Override
    public Path map(Row record) {
        Row[] values = new Row[newFieldNum];
        for (int i = 0; i < fieldTypes.length; i++) {
            values[i] = (Row) record.getField(i, fieldTypes[i]);
        }
        for (int i = 0; i < updatePathIndices.length; i++) {
            Row value = (Row) modifyExpressions[i].evaluate(record);
            updateGlobalVariable(modifyExpressions[i], value);
            values[updatePathIndices[i]] = value;
        }
        return new DefaultPath(values);
    }

    private void updateGlobalVariable(Expression modifyExpression, Row value) {
        // modify global variable to vertex.
        if (modifyExpression instanceof VertexConstructExpression) {
            VertexConstructExpression vertexConstruct = (VertexConstructExpression) modifyExpression;

            List<GlobalVariable> globalVariables = vertexConstruct.getGlobalVariables();
            for (GlobalVariable gv : globalVariables) {
                // index of the global variable
                int index = gv.getIndex();
                VertexType vertexType = ((VertexType) vertexConstruct.getOutputType());
                IType<?> fieldType = vertexType.getType(index);
                Object fieldValue = value.getField(index, fieldType);
                // add field to vertex which will affect all the computing with this vertexId
                int updateIndex = gv.getAddFieldIndex();
                updateIndex = updateIndex >= 0 ? updateIndex : ArrayUtils.indexOf(
                    schemas.getAddingVertexFieldNames(), gv.getName());
                context.addFieldToVertex(((RowVertex) value).getId(), updateIndex, fieldValue);
            }
        }
    }

    @Override
    public List<Expression> getExpressions() {
        return ImmutableList.copyOf(modifyExpressions);
    }

    @Override
    public StepFunction copy(List<Expression> expressions) {
        assert expressions.size() == this.modifyExpressions.length;
        return new StepPathModifyFunction(updatePathIndices, expressions.toArray(new Expression[]{}), fieldTypes);
    }
}
