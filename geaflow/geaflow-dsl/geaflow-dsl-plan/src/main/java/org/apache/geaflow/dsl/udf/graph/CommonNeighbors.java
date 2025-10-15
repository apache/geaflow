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
import org.apache.geaflow.common.tuple.Tuple;
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

@Description(name = "common_neighbors", description = "built-in udga for CommonNeighbors")
public class CommonNeighbors implements AlgorithmUserFunction<Object, Object> {

    private AlgorithmRuntimeContext<Object, Object> context;

    // tuple to store params
    private Tuple<Object, Object> vertices;

    @Override
    public void init(AlgorithmRuntimeContext<Object, Object> context, Object[] params) {
        this.context = context;

        if (params.length != 2) {
            throw new IllegalArgumentException("Only support two arguments, usage: common_neighbors(id_a, id_b)");
        }
        this.vertices = new Tuple<>(
            TypeCastUtil.cast(params[0], context.getGraphSchema().getIdType()),
            TypeCastUtil.cast(params[1], context.getGraphSchema().getIdType())
        );
    }

    @Override
    public void process(RowVertex vertex, Optional<Row> updatedValues, Iterator<Object> messages) {
        if (context.getCurrentIterationId() == 1L) {
            // send message to neighbors if they are vertices in params
            if (vertices.f0.equals(vertex.getId()) || vertices.f1.equals(vertex.getId())) {
                sendMessageToNeighbors(context.loadEdges(EdgeDirection.BOTH), vertex.getId());
            }
        } else if (context.getCurrentIterationId() == 2L) {
            // add to result if received messages from both vertices in params
            Tuple<Boolean, Boolean> received = new Tuple<>(false, false);
            while (messages.hasNext()) {
                Object message = messages.next();
                if (vertices.f0.equals(message)) {
                    received.setF0(true);
                }
                if (vertices.f1.equals(message)) {
                    received.setF1(true);
                }

                if (received.getF0() && received.getF1()) {
                    context.take(ObjectRow.create(vertex.getId()));
                }
            }
        }
    }

    @Override
    public void finish(RowVertex graphVertex, Optional<Row> updatedValues) {
    }

    @Override
    public StructType getOutputType(GraphSchema graphSchema) {
        return new StructType(
            new TableField("id", graphSchema.getIdType(), false)
        );
    }

    private void sendMessageToNeighbors(List<RowEdge> edges, Object message) {
        for (RowEdge rowEdge : edges) {
            context.sendMessage(rowEdge.getTargetId(), message);
        }
    }
}
