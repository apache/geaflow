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
import java.util.stream.Stream;
import org.apache.geaflow.common.type.primitive.StringType;
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

@Description(name = "cc", description = "built-in udga for Connected Components Algorithm")
public class ConnectedComponents implements AlgorithmUserFunction<Object, String> {

    private AlgorithmRuntimeContext<Object, String> context;
    private String outputKeyName = "component";
    private int iteration = 20;

    @Override
    public void init(AlgorithmRuntimeContext<Object, String> context, Object[] parameters) {
        this.context = context;
        if (parameters.length > 2) {
            throw new IllegalArgumentException(
                "Only support zero or more arguments, false arguments "
                    + "usage: func([iteration, [outputKeyName]])");
        }
        if (parameters.length > 0) {
            iteration = Integer.parseInt(String.valueOf(parameters[0]));
        }
        if (parameters.length > 1) {
            outputKeyName = String.valueOf(parameters[1]);
        }
    }

    @Override
    public void process(RowVertex vertex, Optional<Row> updatedValues, Iterator<String> messages) {
        updatedValues.ifPresent(vertex::setValue);
        // Use BOTH direction for undirected graph semantics in connected components
        Stream<RowEdge> stream = context.loadEdges(EdgeDirection.BOTH).stream();
        if (context.getCurrentIterationId() == 1L) {
            String initValue = String.valueOf(vertex.getId());
            sendMessageToNeighbors(stream, initValue);
            context.sendMessage(vertex.getId(), initValue);
            context.updateVertexValue(ObjectRow.create(initValue));
        } else if (context.getCurrentIterationId() < iteration) {
            String minComponent = null;
            while (messages.hasNext()) {
                String next = messages.next();
                if (minComponent == null || compareComponentIds(next, minComponent) < 0) {
                    minComponent = next;
                }
            }

            String currentValue = (String) vertex.getValue().getField(0, StringType.INSTANCE);
            // If found smaller component id, update and propagate
            if (minComponent != null && compareComponentIds(minComponent, currentValue) < 0) {
                sendMessageToNeighbors(stream, minComponent);
                context.sendMessage(vertex.getId(), minComponent);
                context.updateVertexValue(ObjectRow.create(minComponent));
            }
        }
    }

    @Override
    public void finish(RowVertex graphVertex, Optional<Row> updatedValues) {
        updatedValues.ifPresent(graphVertex::setValue);
        String component = (String) graphVertex.getValue().getField(0, StringType.INSTANCE);
        context.take(ObjectRow.create(graphVertex.getId(), component));
    }

    @Override
    public StructType getOutputType(GraphSchema graphSchema) {
        return new StructType(
                new TableField("id", graphSchema.getIdType(), false),
                new TableField(outputKeyName, StringType.INSTANCE, false)
        );
    }

    private void sendMessageToNeighbors(Stream<RowEdge> edges, String message) {
        edges.forEach(rowEdge -> context.sendMessage(rowEdge.getTargetId(), message));
    }

    /**
     * Compare two component IDs. If both are numeric strings, compare them as numbers.
     * Otherwise, fall back to string comparison.
     *
     * @param a first component ID
     * @param b second component ID
     * @return negative if a < b, positive if a > b, zero if equal
     */
    private int compareComponentIds(String a, String b) {
        // Try to compare as numbers first for correct numeric ordering
        try {
            long numA = Long.parseLong(a);
            long numB = Long.parseLong(b);
            return Long.compare(numA, numB);
        } catch (NumberFormatException e) {
            // Fall back to string comparison if not numeric
            return a.compareTo(b);
        }
    }
}
