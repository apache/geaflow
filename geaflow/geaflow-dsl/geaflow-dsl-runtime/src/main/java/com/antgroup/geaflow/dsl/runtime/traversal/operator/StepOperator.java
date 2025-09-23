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
import com.antgroup.geaflow.dsl.common.data.StepRecord;
import com.antgroup.geaflow.dsl.common.types.GraphSchema;
import com.antgroup.geaflow.dsl.common.types.PathType;
import com.antgroup.geaflow.dsl.runtime.traversal.TraversalRuntimeContext;
import org.apache.calcite.rex.RexFieldAccess;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;

public interface StepOperator<IN extends StepRecord, OUT extends StepRecord> extends Serializable {

    /**
     * The operator id.
     */
    long getId();

    /**
     * The operator name.
     */
    String getName();

    /**
     * The init method for step operator.
     * @param context The context for traversal.
     */
    void open(TraversalRuntimeContext context);

    /**
     * Process input record.
     * @param record The input record.
     */
    void process(IN record);

    void finish();

    void close();

    void addNextOperator(StepOperator<OUT, ? extends StepRecord> nextOperator);

    List<StepOperator<OUT, ?>> getNextOperators();

    StepOperator<IN, OUT> withName(String name);

    /**
     * Set the output path schema for the operator.
     * @param outputPath The output path schema.
     */
    StepOperator<IN, OUT> withOutputPathSchema(PathType outputPath);

    /**
     * Set the input path schema for the operator.
     * @param inputPaths The input path schemas for each input.
     */
    StepOperator<IN, OUT> withInputPathSchema(List<PathType> inputPaths);

    default StepOperator<IN, OUT> withInputPathSchema(PathType pathType) {
        return withInputPathSchema(Collections.singletonList(Objects.requireNonNull(pathType)));
    }

    StepOperator<IN, OUT> withOutputType(IType<?> outputType);

    /**
     * Set the origin graph schema.
     * @param graphSchema The origin graph schema defined in the DDL.
     */
    StepOperator<IN, OUT> withGraphSchema(GraphSchema graphSchema);

    /**
     * Set the modified graph schema after the let-global-statement.
     * @param modifyGraphSchema The modified graph schema.
     */
    StepOperator<IN, OUT> withModifyGraphSchema(GraphSchema modifyGraphSchema);

    List<PathType> getInputPathSchemas();

    PathType getOutputPathSchema();

    IType<?> getOutputType();

    GraphSchema getGraphSchema();

    GraphSchema getModifyGraphSchema();

    List<String> getSubQueryNames();

    StepOperator<IN, OUT> copy();

    StepOperator<IN, OUT>  withFilteredFields(Set<RexFieldAccess> fields);
}
