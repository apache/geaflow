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

package org.apache.geaflow.dsl.runtime.traversal.operator;

import java.util.*;
import java.util.stream.Collectors;
import org.apache.geaflow.common.type.IType;
import org.apache.geaflow.dsl.common.binary.encoder.DefaultVertexEncoder;
import org.apache.geaflow.dsl.common.binary.encoder.VertexEncoder;
import org.apache.geaflow.dsl.common.data.*;
import org.apache.geaflow.dsl.common.data.StepRecord.StepRecordType;
import org.apache.geaflow.dsl.common.data.impl.ObjectRow;
import org.apache.geaflow.dsl.common.data.impl.VertexEdgeFactory;
import org.apache.geaflow.dsl.common.types.TableField;
import org.apache.geaflow.dsl.common.types.VertexType;
import org.apache.geaflow.dsl.runtime.expression.Expression;
import org.apache.geaflow.dsl.runtime.expression.construct.VertexConstructExpression;
import org.apache.geaflow.dsl.runtime.expression.field.FieldExpression;
import org.apache.geaflow.dsl.runtime.expression.literal.LiteralExpression;
import org.apache.geaflow.dsl.runtime.function.graph.MatchVertexFunction;
import org.apache.geaflow.dsl.runtime.function.graph.MatchVertexFunctionImpl;
import org.apache.geaflow.dsl.runtime.function.table.ProjectFunction;
import org.apache.geaflow.dsl.runtime.function.table.ProjectFunctionImpl;
import org.apache.geaflow.dsl.runtime.traversal.TraversalRuntimeContext;
import org.apache.geaflow.dsl.runtime.traversal.data.EdgeGroup;
import org.apache.geaflow.dsl.runtime.traversal.data.EdgeGroupRecord;
import org.apache.geaflow.dsl.runtime.traversal.data.IdOnlyVertex;
import org.apache.geaflow.dsl.runtime.traversal.data.VertexRecord;
import org.apache.geaflow.dsl.runtime.traversal.path.ITreePath;
import org.apache.geaflow.metrics.common.MetricNameFormatter;
import org.apache.geaflow.metrics.common.api.Histogram;

public class MatchVertexOperator extends AbstractStepOperator<MatchVertexFunction, StepRecord,
    VertexRecord> implements LabeledStepOperator {

    private Histogram loadVertexRt;

    private final boolean isOptionMatch;

    private Set<Object> idSet;

    //private List<RexFieldAccess> filteredFields;
    private Map<String, ProjectFunction> projectFunctions = new HashMap<>();
    private Map<String, List<TableField>> tableOutputTypes = new HashMap<>();
    //也可以用这两个变量保证每个节点/边匹配只会被初始化一次

    public MatchVertexOperator(long id, MatchVertexFunction function) {
        super(id, function);
        if (function instanceof MatchVertexFunctionImpl) {
            isOptionMatch = ((MatchVertexFunctionImpl) function).isOptionalMatchVertex();
            idSet = ((MatchVertexFunctionImpl) function).getIdSet();
        } else {
            isOptionMatch = false;
        }
    }

    @Override
    public void open(TraversalRuntimeContext context) {
        super.open(context);
        loadVertexRt = metricGroup.histogram(MetricNameFormatter.loadVertexTimeRtName(getName()));
    }

    @SuppressWarnings("unchecked")
    @Override
    protected void processRecord(StepRecord record) {
        if (record.getType() == StepRecordType.VERTEX) {
            processVertex((VertexRecord) record);
        } else {
            EdgeGroupRecord edgeGroupRecord = (EdgeGroupRecord) record;
            processEdgeGroup(edgeGroupRecord);
        }
    }

    private RowVertex projectVertex(RowVertex vertex) {
        if (vertex == null) {  //找不到符合条件节点，无法映射
            return null;
        }

        String compactedVertexLabel = vertex.getLabel();  // 原始变量
        for (String addingName: addingVertexFieldNames) {
            compactedVertexLabel += "_" + addingName;  // 将addingVariable后的label设为新的一类
        }

        // 当前表projectFunction未定义
        if (this.projectFunctions.get(compactedVertexLabel) == null) {
            initializeProject(vertex, compactedVertexLabel, addingVertexFieldTypes, addingVertexFieldNames);
        }

        //进行projection
        ProjectFunction currentProjectFunction = this.projectFunctions.get(compactedVertexLabel);
        ObjectRow projectVertex = (ObjectRow) currentProjectFunction.project(vertex); //通过project进行属性筛选
        RowVertex vertexDecoded = (RowVertex) projectVertex.getField(0, null);


        //需要重构Fields，以定义VertexType，然后再进行encode
        VertexType vertexType = new VertexType(this.tableOutputTypes.get(compactedVertexLabel));
        VertexEncoder encoder = new DefaultVertexEncoder(vertexType);
        return encoder.encode(vertexDecoded);
    }

    private void initializeProject(RowVertex vertex, String compactedLabel,
                                   IType<?>[] globalTypes, String[] globalNames) {
        List<TableField> graphSchemaFieldList = graphSchema.getFields();  //这里是图中的所有表集合

        List<TableField> fieldsOfTable;  //这里的是一张表里的所有字段

        List<TableField> tableOutputType = new ArrayList<>(); //记录新表格所有字段所包括的输出Type

        //提取当前表格内，使用到的字段集合。
        Set<String> fieldNames = (this.fields == null)
                ? Collections.emptySet()
                : this.fields.stream()
                .map(e -> e.getField().getName())
                .collect(Collectors.toSet());


        //对于每个表，都需要一个expression
        List<Expression> expressions = new ArrayList<>();  //对于每个表，都需要一个expression
        String vertexLabel = vertex.getLabel();

        for (TableField tableField : graphSchemaFieldList) {  //枚举所有table，并构造List<Expression>
            if (vertexLabel.equals(tableField.getName())) {  //table名匹配 (如都为`person`)

                List<Expression> inputs = new ArrayList<>();
                fieldsOfTable = ((VertexType)tableField.getType()).getFields();

                for (int i = 0; i < fieldsOfTable.size(); i++) { //枚举表格内不同字段，并做属性筛选
                    TableField column = fieldsOfTable.get(i);
                    String columnName = column.getName();

                    //标准化，将形如personId改为id
                    if (columnName.startsWith(vertexLabel)) {
                        String suffix = columnName.substring(vertexLabel.length());
                        if (!suffix.isEmpty()) {
                            suffix = Character.toLowerCase(suffix.charAt(0)) + suffix.substring(1);
                            columnName = suffix;
                        }
                    }

                    if (fieldNames.contains(columnName) || columnName.equals("id")) {  //存在已经筛选出的字段或是特殊的Id字段
                        inputs.add(new FieldExpression(null, i, column.getType()));
                        tableOutputType.add(column);
                    } else if (columnName.equals("~label")) {  //补充label
                        inputs.add(new LiteralExpression(vertex.getLabel(), column.getType()));
                        tableOutputType.add(column);
                    } else {  //被剔除掉的特征需要使用null占位
                        inputs.add(new LiteralExpression(null, column.getType()));
                        tableOutputType.add(column);
                    }
                }

                if (globalNames.length > 0) { // 存在新的变量
                    for (int j = 0; j < globalNames.length; j++) {
                        int fieldIndex = j + fieldsOfTable.size();
                        inputs.add(new FieldExpression(null, fieldIndex, globalTypes[j]));
                        tableOutputType.add(new TableField(globalNames[j], globalTypes[j]));
                    }
                }

                expressions.add(new VertexConstructExpression(inputs, null, new VertexType(tableOutputType)));
            }
        }

        //封装映射函数
        ProjectFunction projectFunction = new ProjectFunctionImpl(expressions);

        //储存project预备阶段的中间值（使用复合标签）
        this.projectFunctions.put(compactedLabel, projectFunction);
        this.tableOutputTypes.put(compactedLabel, tableOutputType);
    }

    private void processVertex(VertexRecord vertexRecord) {
        RowVertex vertex = vertexRecord.getVertex();
        if (vertex instanceof IdOnlyVertex && needLoadVertex(vertex.getId())) {
            long startTs = System.currentTimeMillis();
            vertex = context.loadVertex(vertex.getId(),
                function.getVertexFilter(),
                graphSchema,
                addingVertexFieldTypes);

            // 如果没有字段，说明未能成功识别该节点（可能由于嵌套），则不进行投影
            if (fields != null) {
                vertex = projectVertex(vertex);  //通过字段进行筛选
            }
            loadVertexRt.update(System.currentTimeMillis() - startTs);
            if (vertex == null && !isOptionMatch) {
                // load a non-exists vertex, just skip.
                return;
            }
        }

        if (vertex != null) {
            if (!function.getVertexTypes().isEmpty()
                && !function.getVertexTypes().contains(vertex.getBinaryLabel())) {
                // filter by the vertex types.
                return;
            }
            if (!idSet.isEmpty() && !idSet.contains(vertex.getId())) {
                return;
            }
            vertex = alignToOutputSchema(vertex);
        }

        ITreePath currentPath;
        if (needAddToPath) {
            currentPath = vertexRecord.getTreePath().extendTo(vertex);
        } else {
            currentPath = vertexRecord.getTreePath();
        }
        if (vertex == null) {
            vertex = VertexEdgeFactory.createVertex((VertexType) getOutputType());
        }
        collect(VertexRecord.of(vertex, currentPath));
    }

    private void processEdgeGroup(EdgeGroupRecord edgeGroupRecord) {
        EdgeGroup edgeGroup = edgeGroupRecord.getEdgeGroup();
        for (RowEdge edge : edgeGroup) {
            Object targetId = edge.getTargetId();
            // load targetId.
            RowVertex vertex = context.loadVertex(targetId, function.getVertexFilter(), graphSchema, addingVertexFieldTypes);
            if (vertex != null) {
                ITreePath treePath = edgeGroupRecord.getPathById(targetId);
                // set current vertex.
                context.setVertex(vertex);
                // process new vertex.
                processVertex(VertexRecord.of(vertex, treePath));
            } else if (isOptionMatch) {
                vertex = VertexEdgeFactory.createVertex((VertexType) getOutputType());
                ITreePath treePath = edgeGroupRecord.getPathById(targetId);
                // set current vertex.
                context.setVertex(vertex);
                // process new vertex.
                processVertex(VertexRecord.of(null, treePath));
            }
        }
    }

    private boolean needLoadVertex(Object vertexId) {
        // skip load virtual id.
        return !(vertexId instanceof VirtualId);
    }

    @Override
    public void close() {

    }

    @Override
    public StepOperator<StepRecord, VertexRecord> copyInternal() {
        return new MatchVertexOperator(id, function);
    }

    @Override
    public String getLabel() {
        return function.getLabel();
    }

    @Override
    public String toString() {
        StringBuilder str = new StringBuilder();
        str.append(getName());
        String label = getLabel();
        str.append(" [").append(label).append("]");
        return str.toString();
    }
}
