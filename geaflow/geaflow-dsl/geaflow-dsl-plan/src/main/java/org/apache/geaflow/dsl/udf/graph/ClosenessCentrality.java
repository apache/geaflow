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
import java.util.List;
import java.util.Optional;
import org.apache.geaflow.common.type.primitive.DoubleType;
import org.apache.geaflow.common.type.primitive.LongType;
import org.apache.geaflow.dsl.common.algo.AlgorithmRuntimeContext;
import org.apache.geaflow.dsl.common.algo.AlgorithmUserFunction;
import org.apache.geaflow.dsl.common.data.Row;
import org.apache.geaflow.dsl.common.data.RowEdge;
import org.apache.geaflow.dsl.common.data.RowVertex;
import org.apache.geaflow.dsl.common.data.impl.ObjectRow;
import org.apache.geaflow.dsl.common.function.Description;
import org.apache.geaflow.dsl.common.types.GraphSchema;
import org.apache.geaflow.dsl.common.types.StructType;
import org.apache.geaflow.dsl.common.types.TableField;
import org.apache.geaflow.dsl.common.util.TypeCastUtil;
import org.apache.geaflow.model.graph.edge.EdgeDirection;

@Description(name = "closeness_centrality", description = "built-in udga for ClosenessCentrality")
public class ClosenessCentrality implements AlgorithmUserFunction<Object, Long> {

    private AlgorithmRuntimeContext context;
    private Object sourceId;

    @Override
    public void init(AlgorithmRuntimeContext context, Object[] params) {
        this.context = context;
        if (params.length != 1) {
            throw new IllegalArgumentException("Only support one arguments, usage: func(sourceId)");
        }
        this.sourceId = TypeCastUtil.cast(params[0], context.getGraphSchema().getIdType());
    }

    @Override
    public void process(RowVertex vertex, Optional<Row> updatedValues, Iterator<Long> messages) {
        updatedValues.ifPresent(vertex::setValue);
        List<RowEdge> edges = context.loadEdges(EdgeDirection.OUT);
        if (context.getCurrentIterationId() == 1L) {
            context.sendMessage(vertex.getId(), 1L);
            context.sendMessage(sourceId, 1L);
        } else if (context.getCurrentIterationId() == 2L) {
            context.updateVertexValue(ObjectRow.create(0L, 0L));
            if (vertex.getId().equals(sourceId)) {
                long vertexNum = -2L;
                while (messages.hasNext()) {
                    messages.next();
                    vertexNum++;
                }
                context.updateVertexValue(ObjectRow.create(0L, vertexNum));
                sendMessageToNeighbors(edges, 1L);
            }
        } else {
            if (vertex.getId().equals(sourceId)) {
                long sum = (long) vertex.getValue().getField(0, LongType.INSTANCE);
                while (messages.hasNext()) {
                    sum += messages.next();
                }
                long vertexNum = (long) vertex.getValue().getField(1, LongType.INSTANCE);
                context.updateVertexValue(ObjectRow.create(sum, vertexNum));
            } else {
                if (((long) vertex.getValue().getField(1, LongType.INSTANCE)) < 1) {
                    Long meg = messages.next();
                    context.sendMessage(sourceId, meg);
                    sendMessageToNeighbors(edges, meg + 1);
                    context.updateVertexValue(ObjectRow.create(0L, 1L));
                }
            }
        }

    }

    @Override
    public void finish(RowVertex graphVertex, Optional<Row> updatedValues) {
        updatedValues.ifPresent(graphVertex::setValue);
        if (graphVertex.getId().equals(sourceId)) {
            long len = (long) graphVertex.getValue().getField(0, LongType.INSTANCE);
            long num = (long) graphVertex.getValue().getField(1, LongType.INSTANCE);
            context.take(ObjectRow.create(graphVertex.getId(), (double) num / len));
        }
    }

    @Override
    public StructType getOutputType(GraphSchema graphSchema) {
        return new StructType(
            new TableField("id", graphSchema.getIdType(), false),
            new TableField("cc", DoubleType.INSTANCE, false)
        );
    }

    private void sendMessageToNeighbors(List<RowEdge> edges, Long message) {
        for (RowEdge rowEdge : edges) {
            context.sendMessage(rowEdge.getTargetId(), message);
        }
    }
}
