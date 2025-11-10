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
import org.apache.geaflow.dsl.common.binary.encoder.DefaultEdgeEncoder;
import org.apache.geaflow.dsl.common.binary.encoder.EdgeEncoder;
import org.apache.geaflow.dsl.common.data.RowEdge;
import org.apache.geaflow.dsl.common.data.impl.ObjectRow;
import org.apache.geaflow.dsl.common.types.EdgeType;
import org.apache.geaflow.dsl.common.types.GraphSchema;
import org.apache.geaflow.dsl.common.types.TableField;
import org.apache.geaflow.dsl.runtime.expression.*;
import org.apache.geaflow.dsl.runtime.expression.construct.EdgeConstructExpression;
import org.apache.geaflow.dsl.runtime.expression.field.FieldExpression;
import org.apache.geaflow.dsl.runtime.expression.literal.LiteralExpression;
import org.apache.geaflow.dsl.runtime.function.table.ProjectFunction;
import org.apache.geaflow.dsl.runtime.function.table.ProjectFunctionImpl;

/**
 * Utility class for projecting edges with field pruning.
 */
public class EdgeProjectorUtil {

    private static final String SOURCE_ID = "srcId";
    private static final String TARGET_ID = "targetId";
    private static final String LABEL = "~label";

    private final Map<String, ProjectFunction> projectFunctions;
    private final Map<String, List<TableField>> tableOutputTypes;
    private final GraphSchema graphSchema;
    private final Set<RexFieldAccess> fields;
    private final IType<?> outputType;

    /**
     * Constructs an EdgeProjector with specified parameters.
     *
     * @param graphSchema The graph schema containing all vertex and edge type definitions
     * @param fields The set of fields to be included in the projection, null means no filtering
     * @param outputType The output type of the edge, must be an EdgeType
     */
    public EdgeProjectorUtil(GraphSchema graphSchema,
                         Set<RexFieldAccess> fields,
                         IType<?> outputType) {
        this.graphSchema = graphSchema;
        this.fields = fields;
        this.outputType = outputType;
        this.projectFunctions = new HashMap<>();
        this.tableOutputTypes = new HashMap<>();

        if (!(outputType instanceof EdgeType)) {
            throw new IllegalArgumentException("Unsupported type: " + outputType.getClass());
        }
    }

    /**
     * Projects an edge by filtering fields based on the required field set.
     *
     * @param edge The input edge to be projected
     * @return The projected edge with only required fields, or null if input is null
     */
    public RowEdge projectEdge(RowEdge edge) {
        if (edge == null) {
            return null;
        }

        String edgeLabel = edge.getLabel();

        // Initialize project function for this edge label if not exists
        if (this.projectFunctions.get(edgeLabel) == null) {
            initializeProject(
                    edge,      // edge: The edge instance used for schema inference
                    edgeLabel  // edgeLabel: The label of the edge for unique identification
            );
        }

        // Utilize project functions to filter fields
        ProjectFunction currentProjectFunction = this.projectFunctions.get(edgeLabel);
        ObjectRow projectEdge = (ObjectRow) currentProjectFunction.project(edge);
        RowEdge edgeDecoded = (RowEdge) projectEdge.getField(0, null);

        EdgeType edgeType = new EdgeType(this.tableOutputTypes.get(edgeLabel), false);
        EdgeEncoder encoder = new DefaultEdgeEncoder(edgeType);
        return encoder.encode(edgeDecoded);
    }

    /**
     * Initializes the project function for a given edge label.
     *
     * @param edge The edge instance used to determine the schema and label
     * @param edgeLabel The label of the edge for unique identification
     */
    private void initializeProject(RowEdge edge, String edgeLabel) {
        List<TableField> graphSchemaFieldList = graphSchema.getFields();

        // Get fields of the output edge type
        List<TableField> fieldsOfTable = ((EdgeType) outputType).getFields();

        // Extract field names from RexFieldAccess list into a set
        Set<String> fieldNames = (this.fields == null)
                ? Collections.emptySet()
                : this.fields.stream()
                .map(e -> e.getField().getName())
                .collect(Collectors.toSet());

        List<Expression> expressions = new ArrayList<>();
        List<TableField> tableOutputType = null;

        // Enumerate list of fields in every table
        for (TableField tableField : graphSchemaFieldList) {
            if (edgeLabel.equals(tableField.getName())) {
                List<Expression> inputs = new ArrayList<>();
                tableOutputType = new ArrayList<>();

                // Enumerate list of fields in the targeted table
                for (int i = 0; i < fieldsOfTable.size(); i++) {
                    TableField column = fieldsOfTable.get(i);
                    String columnName = column.getName();

                    // Normalize: convert fields like `knowsCreationDate` to `creationDate`
                    if (columnName.startsWith(edgeLabel)) {
                        String suffix = columnName.substring(edgeLabel.length());
                        if (!suffix.isEmpty()) {
                            suffix = Character.toLowerCase(suffix.charAt(0)) + suffix.substring(1);
                            columnName = suffix;
                        }
                    }

                    if (fieldNames.contains(columnName)
                            || columnName.equals(SOURCE_ID)
                            || columnName.equals(TARGET_ID)) {
                        // Include a field if it's in fieldNames or is source/target ID column
                        inputs.add(new FieldExpression(null, i, column.getType()));
                        tableOutputType.add(column);
                    } else if (columnName.equals(LABEL)) {
                        // Add edge label for LABEL column
                        inputs.add(new LiteralExpression(edge.getLabel(), column.getType()));
                        tableOutputType.add(column);
                    } else {
                        // Use null placeholder for excluded fields
                        inputs.add(new LiteralExpression(null, column.getType()));
                        tableOutputType.add(column);
                    }
                }

                expressions.add(new EdgeConstructExpression(inputs, new EdgeType(tableOutputType, false)));
            }
        }

        ProjectFunction projectFunction = new ProjectFunctionImpl(expressions);

        // Store project function and output type for this edge label
        this.projectFunctions.put(edgeLabel, projectFunction);
        this.tableOutputTypes.put(edgeLabel, tableOutputType);
    }
}
