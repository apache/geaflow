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

import java.util.List;
import org.apache.geaflow.common.type.IType;
import org.apache.geaflow.dsl.common.types.GraphSchema;
import org.apache.geaflow.dsl.common.types.PathType;

public class FunctionSchemas {

    private final List<PathType> inputPathSchemas;

    private final PathType outputPathSchema;

    private final IType<?> outputType;

    private final GraphSchema graphSchema;

    private final GraphSchema modifyGraphSchema;

    private final IType<?>[] addingVertexFieldTypes;

    private final String[] addingVertexFieldNames;

    public FunctionSchemas(List<PathType> inputPathSchemas,
                           PathType outputPathSchema,
                           IType<?> outputType,
                           GraphSchema graphSchema,
                           GraphSchema modifyGraphSchema,
                           IType<?>[] addingVertexFieldTypes,
                           String[] addingVertexFieldNames) {
        this.inputPathSchemas = inputPathSchemas;
        this.outputPathSchema = outputPathSchema;
        this.outputType = outputType;
        this.graphSchema = graphSchema;
        this.modifyGraphSchema = modifyGraphSchema;
        this.addingVertexFieldTypes = addingVertexFieldTypes;
        this.addingVertexFieldNames = addingVertexFieldNames;
    }

    public List<PathType> getInputPathSchemas() {
        return inputPathSchemas;
    }

    public PathType getOutputPathSchema() {
        return outputPathSchema;
    }

    public IType<?> getOutputType() {
        return outputType;
    }

    public GraphSchema getGraphSchema() {
        return graphSchema;
    }

    public GraphSchema getModifyGraphSchema() {
        return modifyGraphSchema;
    }

    public IType<?>[] getAddingVertexFieldTypes() {
        return addingVertexFieldTypes;
    }

    public String[] getAddingVertexFieldNames() {
        return addingVertexFieldNames;
    }
}
