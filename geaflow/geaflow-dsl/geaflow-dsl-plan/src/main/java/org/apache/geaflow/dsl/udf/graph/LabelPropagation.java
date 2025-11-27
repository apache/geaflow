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
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
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

/**
 * Label Propagation Algorithm (LPA) for community detection.
 *
 * <p>This algorithm assigns labels to vertices based on the most frequent label
 * among their neighbors. It uses an iterative approach where vertices adopt the
 * label that appears most frequently among their neighbors. In case of ties,
 * the smallest label value is selected.</p>
 *
 * <p><b>Performance Optimization:</b> This implementation uses change detection to minimize
 * communication overhead. Vertices only propagate their label to neighbors when it changes,
 * significantly reducing message volume in later iterations when the algorithm stabilizes.
 * This optimization makes the algorithm efficient for large-scale graphs.</p>
 *
 * <p>Parameters:</p>
 * <ul>
 *   <li>iterations (optional): Maximum number of iterations (default: 100)</li>
 *   <li>outputFieldName (optional): Name of the output field (default: "label")</li>
 * </ul>
 *
 * <p>Example usage:</p>
 * <pre>
 * CALL lpa(100, 'label') YIELD (id, label)
 * RETURN id, label
 * ORDER BY id;
 * </pre>
 */
@Description(name = "lpa", description = "built-in udga for label propagation algorithm")
public class LabelPropagation implements AlgorithmUserFunction<Object, String> {

    private AlgorithmRuntimeContext<Object, String> context;
    private String outputFieldName = "label";
    private int iterations = 100;

    @Override
    public void init(AlgorithmRuntimeContext<Object, String> context, Object[] parameters) {
        this.context = context;

        if (parameters.length > 2) {
            throw new IllegalArgumentException(
                "Only support zero or two arguments, usage: lpa([iterations, [outputFieldName]])");
        }

        if (parameters.length > 0) {
            this.iterations = Integer.parseInt(String.valueOf(parameters[0]));
        }

        if (parameters.length > 1) {
            this.outputFieldName = String.valueOf(parameters[1]);
        }
    }

    @Override
    public void process(RowVertex vertex, Optional<Row> updatedValues, Iterator<String> messages) {
        updatedValues.ifPresent(vertex::setValue);
        List<RowEdge> edges = new ArrayList<>(context.loadEdges(EdgeDirection.BOTH));

        if (context.getCurrentIterationId() == 1L) {
            // First iteration: initialize label with vertex ID
            String initValue = String.valueOf(vertex.getId());
            sendMessageToNeighbors(edges, initValue);
            context.sendMessage(vertex.getId(), initValue);
            context.updateVertexValue(ObjectRow.create(initValue));
        } else if (context.getCurrentIterationId() < iterations) {
            // Subsequent iterations: adopt most frequent label from neighbors

            // Collect and count neighbor labels
            Map<String, Long> labelCount = new HashMap<>();
            while (messages.hasNext()) {
                String label = messages.next();
                labelCount.merge(label, 1L, Long::sum);
            }

            if (!labelCount.isEmpty()) {
                // Find the most frequent label (smallest in case of tie)
                String currentLabel = (String) vertex.getValue().getField(0,
                    context.getGraphSchema().getIdType());
                String newLabel = findMostFrequentLabel(labelCount, currentLabel);

                // Update and propagate if label changed
                if (!newLabel.equals(currentLabel)) {
                    sendMessageToNeighbors(edges, newLabel);
                    context.sendMessage(vertex.getId(), newLabel);
                    context.updateVertexValue(ObjectRow.create(newLabel));
                }
            }
        }
    }

    @Override
    public void finish(RowVertex vertex, Optional<Row> updatedValues) {
        updatedValues.ifPresent(vertex::setValue);
        String label = (String) vertex.getValue().getField(0, context.getGraphSchema().getIdType());
        context.take(ObjectRow.create(vertex.getId(), label));
    }

    @Override
    public StructType getOutputType(GraphSchema graphSchema) {
        return new StructType(
            new TableField("id", graphSchema.getIdType(), false),
            new TableField(outputFieldName, graphSchema.getIdType(), false)
        );
    }

    /**
     * Finds the most frequent label from the label count map.
     * In case of ties, returns the smallest label value.
     *
     * @param labelCount Map of labels to their frequencies
     * @param currentLabel Current label of the vertex
     * @return Most frequent label (smallest in case of tie)
     */
    private String findMostFrequentLabel(Map<String, Long> labelCount, String currentLabel) {
        if (labelCount.isEmpty()) {
            return currentLabel;
        }

        // Find maximum frequency
        long maxCount = labelCount.values().stream()
            .max(Long::compareTo)
            .orElse(0L);

        // Find label with maximum frequency (smallest if tie)
        String bestLabel = currentLabel;
        for (Map.Entry<String, Long> entry : labelCount.entrySet()) {
            if (entry.getValue() == maxCount) {
                if (bestLabel == null || entry.getKey().compareTo(bestLabel) < 0) {
                    bestLabel = entry.getKey();
                }
            }
        }

        return bestLabel;
    }

    private void sendMessageToNeighbors(List<RowEdge> edges, String message) {
        for (RowEdge edge : edges) {
            context.sendMessage(edge.getTargetId(), message);
        }
    }
}
