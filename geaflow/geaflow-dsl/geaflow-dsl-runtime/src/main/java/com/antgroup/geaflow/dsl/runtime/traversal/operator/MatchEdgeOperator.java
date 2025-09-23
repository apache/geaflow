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

package com.antgroup.geaflow.dsl.runtime.traversal.operator;

import com.antgroup.geaflow.common.type.IType;
import com.antgroup.geaflow.dsl.common.data.RowEdge;
import com.antgroup.geaflow.dsl.common.data.RowVertex;
import com.antgroup.geaflow.dsl.common.data.impl.ObjectRow;
import com.antgroup.geaflow.dsl.common.types.EdgeType;
import com.antgroup.geaflow.dsl.common.types.TableField;
import com.antgroup.geaflow.dsl.common.types.VertexType;
import com.antgroup.geaflow.dsl.runtime.expression.Expression;
import com.antgroup.geaflow.dsl.runtime.expression.construct.EdgeConstructExpression;
import com.antgroup.geaflow.dsl.runtime.expression.construct.VertexConstructExpression;
import com.antgroup.geaflow.dsl.runtime.expression.field.FieldExpression;
import com.antgroup.geaflow.dsl.runtime.expression.literal.LiteralExpression;
import com.antgroup.geaflow.dsl.runtime.function.graph.MatchEdgeFunction;
import com.antgroup.geaflow.dsl.runtime.function.graph.MatchEdgeFunctionImpl;
import com.antgroup.geaflow.dsl.runtime.function.table.ProjectFunction;
import com.antgroup.geaflow.dsl.runtime.function.table.ProjectFunctionImpl;
import com.antgroup.geaflow.dsl.runtime.traversal.TraversalRuntimeContext;
import com.antgroup.geaflow.dsl.runtime.traversal.data.EdgeGroup;
import com.antgroup.geaflow.dsl.runtime.traversal.data.EdgeGroupRecord;
import com.antgroup.geaflow.dsl.runtime.traversal.data.VertexRecord;
import com.antgroup.geaflow.dsl.runtime.traversal.path.ITreePath;
import com.antgroup.geaflow.dsl.sqlnode.SqlMatchEdge.EdgeDirection;
import com.antgroup.geaflow.metrics.common.MetricNameFormatter;
import com.antgroup.geaflow.metrics.common.api.Histogram;

import java.util.*;
import java.util.stream.Collectors;

public class MatchEdgeOperator extends AbstractStepOperator<MatchEdgeFunction, VertexRecord, EdgeGroupRecord>
    implements LabeledStepOperator {

    private Histogram loadEdgeHg;
    private Histogram loadEdgeRt;

    private final boolean isOptionMatch;

    public MatchEdgeOperator(long id, MatchEdgeFunction function) {
        super(id, function);
        isOptionMatch = function instanceof MatchEdgeFunctionImpl
            && ((MatchEdgeFunctionImpl) function).isOptionalMatchEdge();
    }

    @Override
    public void open(TraversalRuntimeContext context) {
        super.open(context);
        this.loadEdgeHg = metricGroup.histogram(MetricNameFormatter.loadEdgeCountRtName(getName()));
        this.loadEdgeRt = metricGroup.histogram(MetricNameFormatter.loadEdgeTimeRtName(getName()));
    }

    private RowEdge projectEdge(RowEdge edge) {
        List<TableField> graphSchemaFieldList = graphSchema.getFields();  //这里是图中的所有表集合

        IType<?> outputType = this.getOutputType();
        List<TableField> fieldsOfTable;  //这里的是一张表里的所有字段
        if (outputType instanceof EdgeType) {
            fieldsOfTable = ((EdgeType) outputType).getFields();
        } else {
            throw new IllegalArgumentException("Unsupported type: " + outputType.getClass());
        }

        //提取当前表格内，使用到的字段集合。
        Set<String> fieldNames = this.fields.stream()
                .map(e -> e.getField().getName())
                .collect(Collectors.toSet());


        List<Expression> expressions = new ArrayList<>();  //对于每个表，都需要一个expression
        for (TableField tableField : graphSchemaFieldList) {  //枚举所有table，并构造List<Expression>
            if (edge.getLabel().equals(tableField.getName())){  //table名匹配 (如都为`knows`)

                List<Expression> inputs = new ArrayList<>();
                List<TableField> tableOutputType = new ArrayList<>(); //记录新表格所有字段所包括的输出Type

                for (int i = 0; i < fieldsOfTable.size(); i++) { //枚举表格内不同字段，并做属性筛选
                    TableField column = fieldsOfTable.get(i);
                    String columnName = column.getName();
                    if (fieldNames.contains(columnName) || columnName.equals("srcId")
                            || columnName.equals("targetId")) {  //存在已经筛选出的字段
                        inputs.add(new FieldExpression(null, i, column.getType()));
                        tableOutputType.add(column);
                    }
                    else if (columnName.equals("~label")) {  //补充label
                        inputs.add(new LiteralExpression(edge.getLabel(), column.getType()));
                        tableOutputType.add(column);
                    }
                }

                expressions.add(new EdgeConstructExpression(inputs, new EdgeType(tableOutputType, false)));
            }
        }

        ProjectFunction projectFunction = new ProjectFunctionImpl(expressions);
        ObjectRow projectEdge = (ObjectRow) projectFunction.project(edge);
        return (RowEdge) projectEdge.getField(0, null);
    }


    @Override
    public void processRecord(VertexRecord vertex) {
        long startTs = System.currentTimeMillis();
        EdgeGroup loadEdges = context.loadEdges(function.getEdgesFilter());
        loadEdgeRt.update(System.currentTimeMillis() - startTs);
        loadEdges = loadEdges.map(this::alignToOutputSchema);
        // filter by edge types if exists.
        EdgeGroup edgeGroup = loadEdges;
        if (!function.getEdgeTypes().isEmpty()) {
            edgeGroup = loadEdges.filter(edge ->

                function.getEdgeTypes().contains(edge.getBinaryLabel()));
        }
        Map<Object, ITreePath> targetTreePaths = new HashMap<>();

        // generate new paths.
        if (needAddToPath) {
            int numEdge = 0;
            for (RowEdge edge : edgeGroup) {
                edge =  projectEdge(edge); //替换原有变

                // add edge to path.
                if (!targetTreePaths.containsKey(edge.getTargetId())) {
                    ITreePath newPath = vertex.getTreePath().extendTo(edge);
                    targetTreePaths.put(edge.getTargetId(), newPath);
                } else {
                    ITreePath treePath = targetTreePaths.get(edge.getTargetId());
                    treePath.getEdgeSet().addEdge(edge);
                }
                numEdge++;
            }
            if (numEdge == 0 && isOptionMatch) {
                ITreePath newPath = vertex.getTreePath().extendTo((RowEdge) null);
                targetTreePaths.put(null, newPath);
            }
            loadEdgeHg.update(numEdge);
        } else {
            if (!vertex.isPathEmpty()) { // inherit input path.
                int numEdge = 0;
                for (RowEdge edge : edgeGroup) {
                    targetTreePaths.put(edge.getTargetId(), vertex.getTreePath());
                    numEdge++;
                }
                if (numEdge == 0 && isOptionMatch) {
                    targetTreePaths.put(null, vertex.getTreePath());
                }
                loadEdgeHg.update(numEdge);
            }
        }
        EdgeGroupRecord edgeGroupRecord = EdgeGroupRecord.of(edgeGroup, targetTreePaths);
        collect(edgeGroupRecord);
    }

    @Override
    public String getLabel() {
        return function.getLabel();
    }

    @Override
    public StepOperator<VertexRecord, EdgeGroupRecord> copyInternal() {
        return new MatchEdgeOperator(id, function);
    }

    @Override
    public String toString() {
        StringBuilder str = new StringBuilder();
        str.append(getName());
        EdgeDirection direction = getFunction().getDirection();
        str.append("(").append(direction).append(")");
        String label = getLabel();
        str.append(" [").append(label).append("]");
        return str.toString();
    }
}
