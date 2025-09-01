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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import org.apache.geaflow.common.type.primitive.DoubleType;
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
import org.apache.geaflow.model.graph.edge.EdgeDirection;

@Description(name = "page_rank", description = "built-in udga for PageRank")
public class PageRank implements AlgorithmUserFunction<Object, Double> {

    private AlgorithmRuntimeContext context;
    private double alpha = 0.85;
    private double convergence = 0.01;
    private int iteration = 20;

    @Override
    public void init(AlgorithmRuntimeContext context, Object[] parameters) {
        this.context = context;
        if (parameters.length > 3) {
            throw new IllegalArgumentException(
                "Only support zero or more arguments, false arguments "
                    + "usage: func([alpha, [convergence, [max_iteration]]])");
        }
        if (parameters.length > 0) {
            alpha = Double.parseDouble(String.valueOf(parameters[0]));
        }
        if (parameters.length > 1) {
            convergence = Double.parseDouble(String.valueOf(parameters[1]));
        }
        if (parameters.length > 2) {
            iteration = Integer.parseInt(String.valueOf(parameters[2]));
        }
    }

    @Override
    public void process(RowVertex vertex, Optional<Row> updatedValues, Iterator<Double> messages) {
        updatedValues.ifPresent(vertex::setValue);
        List<RowEdge> outEdges = new ArrayList<>(context.loadEdges(EdgeDirection.OUT));
        if (context.getCurrentIterationId() == 1L) {
            double initValue = 1.0;
            sendMessageToNeighbors(outEdges, 1.0 / outEdges.size());
            context.sendMessage(vertex.getId(), -1.0);
            context.updateVertexValue(ObjectRow.create(initValue));
        } else if (context.getCurrentIterationId() < iteration) {
            double sum = 0.0;
            while (messages.hasNext()) {
                double input = (double) messages.next();
                input = input > 0 ? input : 0.0;
                sum += input;
            }
            double pr = (1 - alpha) + (sum * alpha);
            double currentPr = (double) vertex.getValue().getField(0, DoubleType.INSTANCE);
            if (Math.abs(currentPr - pr) > convergence) {
                context.updateVertexValue(ObjectRow.create(pr));
                currentPr = pr;
            }
            sendMessageToNeighbors(outEdges, currentPr / outEdges.size());
            context.sendMessage(vertex.getId(), -1.0);
            context.updateVertexValue(ObjectRow.create(pr));
        }
    }

    @Override
    public void finish(RowVertex vertex, Optional<Row> newValue) {
        if (newValue.isPresent()) {
            double currentPr = (double) newValue.get().getField(0, DoubleType.INSTANCE);
            context.take(ObjectRow.create(vertex.getId(), currentPr));
        }
    }

    @Override
    public StructType getOutputType(GraphSchema graphSchema) {
        return new StructType(
            new TableField("id", graphSchema.getIdType(), false),
            new TableField("pr", DoubleType.INSTANCE, false)
        );
    }

    private void sendMessageToNeighbors(List<RowEdge> outEdges, Object message) {
        for (RowEdge rowEdge : outEdges) {
            context.sendMessage(rowEdge.getTargetId(), message);
        }
    }
}
