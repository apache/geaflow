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

package org.apache.geaflow.dsl.runtime.util;

import java.util.*;
import java.util.stream.Collectors;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.geaflow.common.type.IType;
import org.apache.geaflow.dsl.common.binary.encoder.DefaultVertexEncoder;
import org.apache.geaflow.dsl.common.binary.encoder.VertexEncoder;
import org.apache.geaflow.dsl.common.data.RowVertex;
import org.apache.geaflow.dsl.common.data.impl.ObjectRow;
import org.apache.geaflow.dsl.common.types.GraphSchema;
import org.apache.geaflow.dsl.common.types.TableField;
import org.apache.geaflow.dsl.common.types.VertexType;
import org.apache.geaflow.dsl.runtime.expression.*;
import org.apache.geaflow.dsl.runtime.expression.construct.VertexConstructExpression;
import org.apache.geaflow.dsl.runtime.expression.field.FieldExpression;
import org.apache.geaflow.dsl.runtime.expression.literal.LiteralExpression;
import org.apache.geaflow.dsl.runtime.function.table.ProjectFunction;
import org.apache.geaflow.dsl.runtime.function.table.ProjectFunctionImpl;

/**
 * Utility class for projecting vertices with field pruning.
 */
public class VertexProjectorUtil {

    private static final String ID = "id";
    private static final String LABEL = "~label";

    private final Map<String, ProjectFunction> projectFunctions;
    private final Map<String, List<TableField>> tableOutputTypes;
    private final GraphSchema graphSchema;
    private final Set<RexFieldAccess> fields;
    private final String[] addingVertexFieldNames;
    private final IType<?>[] addingVertexFieldTypes;

    /**
     * Constructs a VertexProjector with specified parameters.
     *
     * @param graphSchema The graph schema containing all vertex and edge type definitions
     * @param fields The set of fields to be included in the projection, null means no filtering
     * @param addingVertexFieldNames The names of additional fields to be added to vertices (e.g., global variables)
     * @param addingVertexFieldTypes The types of additional fields corresponding to addingVertexFieldNames
     */
    public VertexProjectorUtil(GraphSchema graphSchema,
                           Set<RexFieldAccess> fields,
                           String[] addingVertexFieldNames,
                           IType<?>[] addingVertexFieldTypes) {
        this.graphSchema = graphSchema;
        this.fields = fields;
        this.addingVertexFieldNames = addingVertexFieldNames != null ? addingVertexFieldNames : new String[0];
        this.addingVertexFieldTypes = addingVertexFieldTypes != null ? addingVertexFieldTypes : new IType<?>[0];
        this.projectFunctions = new HashMap<>();
        this.tableOutputTypes = new HashMap<>();
    }

    /**
     * Projects a vertex by filtering fields based on the required field set.
     *
     * @param vertex The input vertex to be projected
     * @return The projected vertex with only required fields, or null if input is null
     */
    public RowVertex projectVertex(RowVertex vertex) {
        if (vertex == null) {
            return null;
        }

        // Handle the case of global variables
        String compactedVertexLabel = vertex.getLabel();
        for (String addingName : addingVertexFieldNames) {
            compactedVertexLabel += "_" + addingName;
        }

        // Initialize
        if (this.projectFunctions.get(compactedVertexLabel) == null) {
            initializeProject(vertex, compactedVertexLabel, addingVertexFieldTypes, addingVertexFieldNames);
        }

        // Utilize project functions to filter fields
        ProjectFunction currentProjectFunction = this.projectFunctions.get(compactedVertexLabel);
        ObjectRow projectVertex = (ObjectRow) currentProjectFunction.project(vertex);
        RowVertex vertexDecoded = (RowVertex) projectVertex.getField(0, null);

        VertexType vertexType = new VertexType(this.tableOutputTypes.get(compactedVertexLabel));
        VertexEncoder encoder = new DefaultVertexEncoder(vertexType);
        return encoder.encode(vertexDecoded);
    }

    /**
     * Initializes the project function for a given vertex label.
     *
     * @param vertex The vertex instance used to determine the schema and label
     * @param compactedLabel The vertex label with additional field names appended for unique identification
     * @param globalTypes The types of global variables to be added to the vertex
     * @param globalNames The names of global variables to be added to the vertex
     */
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

        for (TableField tableField : graphSchemaFieldList) {
            if (vertexLabel.equals(tableField.getName())) {
                List<Expression> inputs = new ArrayList<>();
                fieldsOfTable = ((VertexType) tableField.getType()).getFields();

                for (int i = 0; i < fieldsOfTable.size(); i++) {
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
}
