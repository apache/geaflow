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

package org.apache.geaflow.dsl.udf.graph;

import java.util.Iterator;
import java.util.Optional;
import org.apache.geaflow.common.type.primitive.IntegerType;
import org.apache.geaflow.dsl.common.algo.AlgorithmRuntimeContext;
import org.apache.geaflow.dsl.common.algo.AlgorithmUserFunction;
import org.apache.geaflow.dsl.common.data.Row;
import org.apache.geaflow.dsl.common.data.RowVertex;
import org.apache.geaflow.dsl.common.data.impl.ObjectRow;
import org.apache.geaflow.dsl.common.function.Description;
import org.apache.geaflow.dsl.common.types.GraphSchema;
import org.apache.geaflow.dsl.common.types.StructType;
import org.apache.geaflow.dsl.common.types.TableField;
import org.apache.geaflow.model.graph.edge.EdgeDirection;

@Description(name = "degree", description = "built-in udga for Degree")
public class Degree implements AlgorithmUserFunction<Object, Integer> {

    private AlgorithmRuntimeContext<Object, Integer> context;

    @Override
    public void init(AlgorithmRuntimeContext<Object, Integer> context, Object[] params) {
        this.context = context;
        if (params.length > 0) {
            throw new IllegalArgumentException(
                "The degree algorithm takes no arguments, usage: degree()");
        }
    }

    @Override
    public void process(RowVertex vertex, Optional<Row> updatedValues, Iterator<Integer> messages) {
        updatedValues.ifPresent(vertex::setValue);
        // Degree needs no message passing: each vertex can count its own
        // adjacent edges directly in the first (and only) iteration.
        if (context.getCurrentIterationId() == 1L) {
            int inDegree = context.loadEdges(EdgeDirection.IN).size();
            int outDegree = context.loadEdges(EdgeDirection.OUT).size();
            context.updateVertexValue(ObjectRow.create(inDegree, outDegree));
        }
    }

    @Override
    public void finish(RowVertex graphVertex, Optional<Row> updatedValues) {
        updatedValues.ifPresent(graphVertex::setValue);
        int inDegree = (int) graphVertex.getValue().getField(0, IntegerType.INSTANCE);
        int outDegree = (int) graphVertex.getValue().getField(1, IntegerType.INSTANCE);
        context.take(ObjectRow.create(graphVertex.getId(), inDegree, outDegree));
    }

    @Override
    public StructType getOutputType(GraphSchema graphSchema) {
        return new StructType(
            new TableField("id", graphSchema.getIdType(), false),
            new TableField("in_degree", IntegerType.INSTANCE, false),
            new TableField("out_degree", IntegerType.INSTANCE, false)
        );
    }

}
