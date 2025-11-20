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
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import org.apache.geaflow.common.type.primitive.DoubleType;
import org.apache.geaflow.common.type.primitive.IntegerType;
import org.apache.geaflow.common.type.primitive.LongType;
import org.apache.geaflow.common.type.primitive.StringType;
import org.apache.geaflow.dsl.common.algo.AlgorithmRuntimeContext;
import org.apache.geaflow.dsl.common.algo.AlgorithmUserFunction;
import org.apache.geaflow.dsl.common.data.Row;
import org.apache.geaflow.dsl.common.data.RowEdge;
import org.apache.geaflow.dsl.common.data.RowVertex;
import org.apache.geaflow.dsl.common.data.impl.ObjectRow;
import org.apache.geaflow.dsl.common.function.Description;
import org.apache.geaflow.dsl.common.types.GraphSchema;
import org.apache.geaflow.dsl.common.types.ObjectType;
import org.apache.geaflow.dsl.common.types.StructType;
import org.apache.geaflow.dsl.common.types.TableField;
import org.apache.geaflow.model.graph.edge.EdgeDirection;

@Description(name = "cluster_coefficient", description = "built-in udga for ClusterCoefficient.")
public class ClusterCoefficient implements AlgorithmUserFunction<Object, ObjectRow> {

    private AlgorithmRuntimeContext<Object, ObjectRow> context;

    /**
     * only calculate vertices(include neighbors) with this label
     */
    private String vertexType;

    /**
     * default degree threshold is 2
     * (nodes with degree < 2 have clustering coefficient 0)
     */
    private Integer threshold = 2;

    @Override
    public void init(AlgorithmRuntimeContext<Object, ObjectRow> context, Object[] params) {
        this.context = context;
        // Support optional vertex type filtering parameter
        if (params.length >= 1) {
            vertexType = String.valueOf(params[0]);
        }
        // Support minimum degree threshold parameter
        if (params.length >= 2) {
            threshold = Integer.valueOf(params[1].toString());
        }

    }

    @Override
    public void process(RowVertex vertex, Optional<Row> updatedValues, Iterator<ObjectRow> messages) {
        // send current node info to neighbor after filtered
        if (this.context.getCurrentIterationId() == 1L) {
            ObjectRow message = ObjectRow.create(vertex.getId(), vertex.getLabel());
            this.context.loadEdges(EdgeDirection.BOTH)
                    .stream()
                    .map(RowEdge::getTargetId)
                    .forEach(neighbor -> this.context.sendMessage(neighbor, message));
            return;
        }
        // know current node has which neighborhood
        if (this.context.getCurrentIterationId() == 2L) {
            if (vertexType != null && !vertexType.equals(vertex.getLabel())) {
                return;
            }
            List<Long> neighborIds = new ArrayList<>();
            while (messages.hasNext()) {
                ObjectRow message = messages.next();
                String vertexLabel = (String) message.getField(1, StringType.INSTANCE);
                if (vertexType == null) {
                    neighborIds.add((Long) message.getField(0,LongType.INSTANCE));
                } else if (!vertexType.equals(vertexLabel)) {
                    neighborIds.add((Long) message.getField(0,LongType.INSTANCE));
                }
            }
            ObjectRow currentNodeNeighborMessage = ObjectRow.create(neighborIds.size(), neighborIds);
            neighborIds.forEach(neighbor -> this.context.sendMessage(neighbor, currentNodeNeighborMessage));
            if (neighborIds.size() < threshold) {
                this.context.take(ObjectRow.create(vertex.getId(), 0d));
            } else {
                this.context.sendMessage(vertex.getId(), ObjectRow.create(0));
                this.context.updateVertexValue(currentNodeNeighborMessage);
            }
            return;
        }
        // calculate connection count between neighbors
        if (this.context.getCurrentIterationId() == 3L) {
            if (updatedValues.isEmpty()) {
                return;
            }
            vertex.setValue(updatedValues.get());
            long count = 0L;
            Set<Long> currentNodeNeighbor = rowToSet(vertex.getValue());
            while (messages.hasNext()) {
                Set<Long> neighborNodeNeighbor = rowToSet(messages.next());
                neighborNodeNeighbor.retainAll(currentNodeNeighbor);
                count += neighborNodeNeighbor.size();
            }
            this.context.sendMessage(vertex.getId(), ObjectRow.create(0));
            this.context.updateVertexValue(ObjectRow.create(count, currentNodeNeighbor.size()));
            return;
        }
        // calculate final clustering coefficient results
        if (this.context.getCurrentIterationId() == 4L) {
            long count = (long) updatedValues.get().getField(0, LongType.INSTANCE);
            int degree = (int) updatedValues.get().getField(1, IntegerType.INSTANCE);
            double coefficient = count / (degree * (degree - 1.0));
            this.context.take(ObjectRow.create(vertex.getId(), coefficient));
        }
    }


    @Override
    public void finish(RowVertex graphVertex, Optional<Row> updatedValues) {

    }

    @Override
    public StructType getOutputType(GraphSchema graphSchema) {
        return new StructType(
                new TableField("vid", LongType.INSTANCE, false),
                new TableField("coefficient", DoubleType.INSTANCE, false)
        );
    }


    private Set<Long> rowToSet(Row row) {
        int len = (int) row.getField(0, IntegerType.INSTANCE);
        if (len == 0) {
            return Collections.emptySet();
        }
        return new HashSet<>(((List<Long>) row.getField(1, ObjectType.INSTANCE)));
    }


}
