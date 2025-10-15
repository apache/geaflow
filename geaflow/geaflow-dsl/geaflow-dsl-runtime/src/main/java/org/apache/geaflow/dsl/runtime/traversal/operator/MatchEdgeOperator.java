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
import org.apache.geaflow.dsl.common.binary.encoder.DefaultEdgeEncoder;
import org.apache.geaflow.dsl.common.binary.encoder.EdgeEncoder;
import org.apache.geaflow.dsl.common.data.RowEdge;
import org.apache.geaflow.dsl.common.data.impl.ObjectRow;
import org.apache.geaflow.dsl.common.types.EdgeType;
import org.apache.geaflow.dsl.common.types.TableField;
import org.apache.geaflow.dsl.runtime.expression.Expression;
import org.apache.geaflow.dsl.runtime.expression.construct.EdgeConstructExpression;
import org.apache.geaflow.dsl.runtime.expression.field.FieldExpression;
import org.apache.geaflow.dsl.runtime.expression.literal.LiteralExpression;
import org.apache.geaflow.dsl.runtime.function.graph.MatchEdgeFunction;
import org.apache.geaflow.dsl.runtime.function.graph.MatchEdgeFunctionImpl;
import org.apache.geaflow.dsl.runtime.function.table.ProjectFunction;
import org.apache.geaflow.dsl.runtime.function.table.ProjectFunctionImpl;
import org.apache.geaflow.dsl.runtime.traversal.TraversalRuntimeContext;
import org.apache.geaflow.dsl.runtime.traversal.data.EdgeGroup;
import org.apache.geaflow.dsl.runtime.traversal.data.EdgeGroupRecord;
import org.apache.geaflow.dsl.runtime.traversal.data.VertexRecord;
import org.apache.geaflow.dsl.runtime.traversal.path.ITreePath;
import org.apache.geaflow.dsl.sqlnode.SqlMatchEdge.EdgeDirection;
import org.apache.geaflow.metrics.common.MetricNameFormatter;
import org.apache.geaflow.metrics.common.api.Histogram;

public class MatchEdgeOperator extends AbstractStepOperator<MatchEdgeFunction, VertexRecord, EdgeGroupRecord>
    implements LabeledStepOperator {

    private Histogram loadEdgeHg;
    private Histogram loadEdgeRt;

    private final boolean isOptionMatch;

    private ProjectFunction projectFunction = null;
    private List<TableField> tableOutputType = null;
    //也可以用这两个变量保证每个节点/边匹配只会被初始化一次

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
        if (edge == null) {  //找不到符合条件节点，无法映射
            return null;
        }

        if (this.projectFunction == null) {
            initializeProject(edge);
        }

        //进行projection
        ObjectRow projectEdge = (ObjectRow) this.projectFunction.project(edge); //通过project进行属性筛选
        RowEdge edgeDecoded = (RowEdge) projectEdge.getField(0, null);

        //需要重构Fields，以定义EdgeType，然后再进行encode
        EdgeType edgeType = new EdgeType(this.tableOutputType, false);
        EdgeEncoder encoder = new DefaultEdgeEncoder(edgeType);
        return encoder.encode(edgeDecoded);
    }


    private void initializeProject(RowEdge edge) {
        List<TableField> graphSchemaFieldList = graphSchema.getFields();  //这里是图中的所有表集合

        IType<?> outputType = this.getOutputType();
        List<TableField> fieldsOfTable;  //这里的是一张表里的所有字段
        if (outputType instanceof EdgeType) {
            fieldsOfTable = ((EdgeType) outputType).getFields();
        } else {
            throw new IllegalArgumentException("Unsupported type: " + outputType.getClass());
        }

        //提取当前表格内，使用到的字段集合。
        Set<String> fieldNames = (this.fields == null)
                ? Collections.emptySet()
                : this.fields.stream()
                .map(e -> e.getField().getName())
                .collect(Collectors.toSet());


        List<Expression> expressions = new ArrayList<>();  //对于每个表，都需要一个expression
        int[] currentIndicesMapping = new int[graphSchemaFieldList.size()]; //在当前匹配下，原index到裁剪后index的映射
        Arrays.fill(currentIndicesMapping, -1);

        List<TableField> tableOutputType = null;
        for (TableField tableField : graphSchemaFieldList) {  //枚举所有table，并构造List<Expression>
            if (edge.getLabel().equals(tableField.getName())) {  //table名匹配 (如都为`knows`)

                List<Expression> inputs = new ArrayList<>();
                tableOutputType = new ArrayList<>();
                String edgeLabel = edge.getLabel();

                for (int i = 0; i < fieldsOfTable.size(); i++) { //枚举表格内不同字段，并做属性筛选
                    TableField column = fieldsOfTable.get(i);
                    String columnName = column.getName();

                    //标准化，将形如personId改为id
                    if (columnName.startsWith(edgeLabel)) {
                        String suffix = columnName.substring(edgeLabel.length());
                        if (!suffix.isEmpty()) {
                            suffix = Character.toLowerCase(suffix.charAt(0)) + suffix.substring(1);
                            columnName = suffix;
                        }
                    }

                    if (fieldNames.contains(columnName) || columnName.equals("srcId")
                            || columnName.equals("targetId")) {  //存在已经筛选出的字段
                        inputs.add(new FieldExpression(null, i, column.getType()));
                        tableOutputType.add(column);
                        currentIndicesMapping[i] = inputs.size();  //记录原始索引->新索引的映射

                    } else if (columnName.equals("~label")) {  //补充label
                        inputs.add(new LiteralExpression(edge.getLabel(), column.getType()));
                        tableOutputType.add(column);
                        currentIndicesMapping[i] = inputs.size();
                    } else {  //被剔除掉的特征需要使用null占位
                        inputs.add(new LiteralExpression(null, column.getType()));
                        tableOutputType.add(column);
                    }
                }

                expressions.add(new EdgeConstructExpression(inputs, new EdgeType(tableOutputType, false)));
            }
        }

        ProjectFunction projectFunction = new ProjectFunctionImpl(expressions);

        this.projectFunction = projectFunction;
        this.tableOutputType = tableOutputType;

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
                edge =  projectEdge(edge); //替换原有边

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
