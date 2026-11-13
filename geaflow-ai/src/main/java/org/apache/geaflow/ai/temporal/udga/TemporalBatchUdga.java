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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import org.apache.geaflow.ai.temporal.codec.TemporalJsonCodec;
import org.apache.geaflow.ai.temporal.model.FactKey;
import org.apache.geaflow.ai.temporal.semantics.NormalizedMemoryEvent;
import org.apache.geaflow.common.type.primitive.BinaryStringType;
import org.apache.geaflow.common.type.primitive.StringType;
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

/** Event vertices dispatch a finite batch; FactKey vertices persist its complete timeline. */
public final class TemporalBatchUdga implements AlgorithmUserFunction<Object, TemporalMessage>,
    IncrementalAlgorithmUserFunction {

    private AlgorithmRuntimeContext<Object, TemporalMessage> context;

    @Override
    public void init(AlgorithmRuntimeContext<Object, TemporalMessage> context, Object[] params) {
        this.context = context;
    }

    @Override
    public void process(RowVertex vertex, Optional<Row> updatedValues,
                        Iterator<TemporalMessage> messages) {
        String payload = vertex.getValue().getField(0, BinaryStringType.INSTANCE).toString();
        if ("event".equals(vertex.getLabel())) {
            if (context.getCurrentIterationId() == 1L) {
                for (RowEdge edge : context.loadDynamicEdges(EdgeDirection.OUT)) {
                    if ("applies_to".equals(edge.getLabel())) {
                        context.sendMessage(edge.getTargetId(),
                            new TemporalMessage(TemporalGraphSchema.decode(payload)));
                    }
                }
            }
            return;
        }
        FactKey key = FactKeyState.key(new TemporalJsonCodec().parseObject(
            TemporalGraphSchema.decode(payload)));
        FactKeyState state;
        if (updatedValues.isPresent()) {
            state = FactKeyState.fromRow(key, updatedValues.get());
        } else {
            String seed = vertex.getValue().getField(1, BinaryStringType.INSTANCE).toString();
            state = "-".equals(seed) ? FactKeyState.empty(key) : FactKeyState.decode(seed);
            if (!key.equals(state.getKey())) {
                throw new IllegalArgumentException("Seed belongs to another FactKey");
            }
        }
        if (context.getCurrentIterationId() == 1L) {
            context.take(ObjectRow.create(vertex.getId(), state.encode(),
                updatedValues.isPresent() ? "PREVIOUS_ROW" : "SEED_ROW"));
        }
        List<NormalizedMemoryEvent> events = new ArrayList<>();
        while (messages.hasNext()) {
            events.add(new TemporalJsonCodec().decodeEvent(messages.next().getEventJson()));
        }
        state = state.applyBatch(events);
        context.updateVertexValue(state.toRow());
    }

    @Override
    public void finish(RowVertex vertex, Optional<Row> updatedValues) {
        if ("fact_key".equals(vertex.getLabel()) && updatedValues.isPresent()) {
            String payload = vertex.getValue().getField(0, BinaryStringType.INSTANCE).toString();
            FactKey key = FactKeyState.key(new TemporalJsonCodec().parseObject(
                TemporalGraphSchema.decode(payload)));
            context.take(ObjectRow.create(vertex.getId(),
                FactKeyState.fromRow(key, updatedValues.get()).encode(), "RESULT"));
        }
    }

    @Override
    public StructType getOutputType(GraphSchema graphSchema) {
        return new StructType(new TableField("vertex_id", graphSchema.getIdType(), false),
            new TableField("state_payload", StringType.INSTANCE, false),
            new TableField("phase", StringType.INSTANCE, false));
    }
}
