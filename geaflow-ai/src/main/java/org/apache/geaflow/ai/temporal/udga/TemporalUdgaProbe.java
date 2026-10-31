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

package org.apache.geaflow.ai.temporal.udga;

import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import org.apache.geaflow.common.type.primitive.BooleanType;
import org.apache.geaflow.common.type.primitive.IntegerType;
import org.apache.geaflow.dsl.common.algo.AlgorithmRuntimeContext;
import org.apache.geaflow.dsl.common.algo.AlgorithmUserFunction;
import org.apache.geaflow.dsl.common.algo.IncrementalAlgorithmUserFunction;
import org.apache.geaflow.dsl.common.data.Row;
import org.apache.geaflow.dsl.common.data.RowEdge;
import org.apache.geaflow.dsl.common.data.RowVertex;
import org.apache.geaflow.dsl.common.data.impl.ObjectRow;
import org.apache.geaflow.dsl.common.types.GraphSchema;
import org.apache.geaflow.dsl.common.types.StructType;
import org.apache.geaflow.dsl.common.types.TableField;
import org.apache.geaflow.model.graph.edge.EdgeDirection;

public class TemporalUdgaProbe implements
    AlgorithmUserFunction<Object, Integer>,
    IncrementalAlgorithmUserFunction {

    private AlgorithmRuntimeContext<Object, Integer> context;

    @Override
    public void init(
        AlgorithmRuntimeContext<Object, Integer> context,
        Object[] params) {
        this.context = context;
    }

    @Override
    public void process(
        RowVertex vertex,
        Optional<Row> updatedValues,
        Iterator<Integer> messages) {
        if (context.getCurrentIterationId() == 1L) {
            processDynamicEdges(updatedValues);
        } else {
            processMessages(updatedValues, messages);
        }
    }

    private void processDynamicEdges(Optional<Row> updatedValues) {
        int batchCount = updatedValues
            .map(value -> integerField(value, 0))
            .orElse(0) + 1;
        int receivedMessageCount = updatedValues
            .map(value -> integerField(value, 3))
            .orElse(0);
        List<RowEdge> dynamicEdges =
            context.loadDynamicEdges(EdgeDirection.OUT);

        context.updateVertexValue(ObjectRow.create(
            batchCount,
            updatedValues.isPresent(),
            dynamicEdges.size(),
            receivedMessageCount));
        for (RowEdge edge : dynamicEdges) {
            context.sendMessage(edge.getTargetId(), 1);
        }
    }

    private void processMessages(
        Optional<Row> updatedValues,
        Iterator<Integer> messages) {
        int receivedNow = 0;
        while (messages.hasNext()) {
            receivedNow += messages.next();
        }
        if (receivedNow == 0 || !updatedValues.isPresent()) {
            return;
        }

        Row value = updatedValues.get();
        context.updateVertexValue(ObjectRow.create(
            integerField(value, 0),
            booleanField(value, 1),
            integerField(value, 2),
            integerField(value, 3) + receivedNow));
    }

    @Override
    public void finish(
        RowVertex vertex,
        Optional<Row> updatedValues) {
        if (!updatedValues.isPresent()) {
            return;
        }
        Row value = updatedValues.get();
        context.take(ObjectRow.create(
            vertex.getId(),
            integerField(value, 0),
            booleanField(value, 1),
            integerField(value, 2),
            integerField(value, 3)));
    }

    @Override
    public StructType getOutputType(GraphSchema graphSchema) {
        return new StructType(
            new TableField(
                "vertex_id",
                graphSchema.getIdType(),
                false),
            new TableField(
                "batch_count",
                IntegerType.INSTANCE,
                false),
            new TableField(
                "had_previous_value",
                BooleanType.INSTANCE,
                false),
            new TableField(
                "dynamic_edge_count",
                IntegerType.INSTANCE,
                false),
            new TableField(
                "received_message_count",
                IntegerType.INSTANCE,
                false));
    }

    private static int integerField(Row value, int index) {
        return (int) value.getField(index, IntegerType.INSTANCE);
    }

    private static boolean booleanField(Row value, int index) {
        return (boolean) value.getField(index, BooleanType.INSTANCE);
    }
}
