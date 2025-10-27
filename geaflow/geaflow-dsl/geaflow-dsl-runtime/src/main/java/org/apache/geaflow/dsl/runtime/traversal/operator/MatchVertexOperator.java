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
import org.apache.calcite.rex.RexFieldAccess;
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
    VertexRecord> implements FilteredFieldsOperator, LabeledStepOperator {

    private static final String ID = "id";
    private static final String LABEL = "~label";

    private Histogram loadVertexRt;

    private final boolean isOptionMatch;

    private Set<Object> idSet;

    private Set<RexFieldAccess> fields;

    // For each schema type，project will only be initialized once.
    private Map<String, ProjectFunction> projectFunctions = new HashMap<>();
    private Map<String, List<TableField>> tableOutputTypes = new HashMap<>();

    @Override
    public StepOperator<StepRecord, VertexRecord> withFilteredFields(Set<RexFieldAccess> fields) {
        this.fields = fields;
        return this;
    }

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
        if (vertex == null) {
            return null;
        }

        // Handle the case of global variables
        String compactedVertexLabel = vertex.getLabel();
        for (String addingName: addingVertexFieldNames) {
            compactedVertexLabel += "_" + addingName;
        }

        // Initialize
        if (this.projectFunctions.get(compactedVertexLabel) == null) {
            initializeProject(vertex, compactedVertexLabel, addingVertexFieldTypes, addingVertexFieldNames);
        }

        // Utilize project functions to filter Fields
        ProjectFunction currentProjectFunction = this.projectFunctions.get(compactedVertexLabel);
        ObjectRow projectVertex = (ObjectRow) currentProjectFunction.project(vertex);
        RowVertex vertexDecoded = (RowVertex) projectVertex.getField(0, null);

        VertexType vertexType = new VertexType(this.tableOutputTypes.get(compactedVertexLabel));
        VertexEncoder encoder = new DefaultVertexEncoder(vertexType);
        return encoder.encode(vertexDecoded);
    }

    private void initializeProject(RowVertex vertex, String compactedLabel,
                                   IType<?>[] globalTypes, String[] globalNames) {
        List<TableField> graphSchemaFieldList = graphSchema.getFields();

        List<TableField> fieldsOfTable;

        List<TableField> tableOutputType = new ArrayList<>();

        // Extract field names from RexFieldAccess list into a set
        Set<String> fieldNames = (this.fields == null)
                ? Collections.emptySet()
                : this.fields.stream()
                .map(e -> e.getField().getName())
                .collect(Collectors.toSet());


        List<Expression> expressions = new ArrayList<>();
        String vertexLabel = vertex.getLabel();

        for (TableField tableField : graphSchemaFieldList) {  // Enumerate list of fields in every table.
            if (vertexLabel.equals(tableField.getName())) {

                List<Expression> inputs = new ArrayList<>();
                fieldsOfTable = ((VertexType)tableField.getType()).getFields();

                for (int i = 0; i < fieldsOfTable.size(); i++) { // Enumerate list of fields in the targeted table.
                    TableField column = fieldsOfTable.get(i);
                    String columnName = column.getName();

                    // Normalize: convert fields like `personId` to `id`
                    if (columnName.startsWith(vertexLabel)) {
                        String suffix = columnName.substring(vertexLabel.length());
                        if (!suffix.isEmpty()) {
                            suffix = Character.toLowerCase(suffix.charAt(0)) + suffix.substring(1);
                            columnName = suffix;
                        }
                    }

                    if (fieldNames.contains(columnName) || columnName.equals(ID)) {
                        // Include a field if it's in fieldNames or is ID column
                        inputs.add(new FieldExpression(null, i, column.getType()));
                        tableOutputType.add(column);
                    } else if (columnName.equals(LABEL)) {
                        // Add vertex label for LABEL column
                        inputs.add(new LiteralExpression(vertex.getLabel(), column.getType()));
                        tableOutputType.add(column);
                    } else {
                        // Use null placeholder for excluded fields
                        inputs.add(new LiteralExpression(null, column.getType()));
                        tableOutputType.add(column);
                    }
                }

                // Handle additional mapping when all global variables exist
                if (globalNames.length > 0) {
                    for (int j = 0; j < globalNames.length; j++) {
                        int fieldIndex = j + fieldsOfTable.size();
                        inputs.add(new FieldExpression(null, fieldIndex, globalTypes[j]));
                        tableOutputType.add(new TableField(globalNames[j], globalTypes[j]));
                    }
                }

                expressions.add(new VertexConstructExpression(inputs, null, new VertexType(tableOutputType)));
            }
        }

        ProjectFunction projectFunction = new ProjectFunctionImpl(expressions);

        // Store project functions
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

            if (fields != null) {
                vertex = projectVertex(vertex);
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
