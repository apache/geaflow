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
import org.apache.geaflow.common.type.IType;
import org.apache.geaflow.common.type.primitive.BooleanType;
import org.apache.geaflow.dsl.common.algo.AlgorithmRuntimeContext;
import org.apache.geaflow.dsl.common.algo.AlgorithmUserFunction;
import org.apache.geaflow.dsl.common.algo.IncrementalAlgorithmUserFunction;
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

@Description(name = "inc_wcc", description = "built-in udga for WeakConnectedComponents")
public class IncWeakConnectedComponents implements AlgorithmUserFunction<Object, Object>,
    IncrementalAlgorithmUserFunction {

    private AlgorithmRuntimeContext<Object, Object> context;
    private String keyFieldName = "component";
    private int iteration = 20;

    @Override
    public void init(AlgorithmRuntimeContext<Object, Object> context, Object[] parameters) {
        this.context = context;
        if (parameters.length > 2) {
            throw new IllegalArgumentException(
                "Only support zero or more arguments, false arguments "
                    + "usage: func([alpha, [convergence, [max_iteration]]])");
        }
        if (parameters.length > 0) {
            iteration = Integer.parseInt(String.valueOf(parameters[0]));
        }
        if (parameters.length > 1) {
            keyFieldName = String.valueOf(parameters[1]);
        }
    }

    @Override
    public void process(RowVertex vertex, Optional<Row> updatedValues, Iterator<Object> messages) {
        List<RowEdge> edges = new ArrayList<>(context.loadEdges(EdgeDirection.BOTH));
        Object component = null;
        if (updatedValues.isPresent()) {
            component = updatedValues.get().getField(0, ObjectType.INSTANCE);
        }
        if (context.getCurrentIterationId() == 1L) {
            Object initValue = vertex.getId();
            if (component == null || ((Comparable) initValue).compareTo(component) < 0) {
                // In iteration 1, if the vertex is activated for the first time, assign an initial
                // value to it and output. Send this message to its neighbors.
                sendMessageToNeighbors(edges, initValue);
                context.updateVertexValue(ObjectRow.create(initValue, true));
            } else {
                // If the vertex already has a component value, and that value is less than its
                // id, there is no need to output it, as its value has not changed, and there is
                // no need to send messages to its neighbors.
                sendMessageToNeighbors(edges, component);
                context.updateVertexValue(ObjectRow.create(component, false));
            }
        } else if (context.getCurrentIterationId() < iteration) {
            // Find min component in messages.
            Object minComponent = messages.next();
            while (messages.hasNext()) {
                Object next = messages.next();
                if (((Comparable) next).compareTo(minComponent) < 0) {
                    minComponent = next;
                }
            }
            if (component != null) {
                minComponent = ((Comparable) component).compareTo(minComponent) < 0 ? component : minComponent;
            }
            if (!minComponent.equals(component)) {
                // If the min component in messages is smaller than the component in current
                // vertex, send message to its neighbors, update its value and output.
                sendMessageToNeighbors(edges, minComponent);
                context.updateVertexValue(ObjectRow.create(minComponent, true));
            }
        }
    }

    @Override
    public void finish(RowVertex graphVertex, Optional<Row> updatedValues) {
        if (updatedValues.isPresent()) {
            boolean vertexUpdateFlag = (boolean) updatedValues.get().getField(1, BooleanType.INSTANCE);
            // Get the vertex update flag, if it is true, output the component of current vertex.
            // If it is false, it indicates that the component value of the current vertex has not
            // changed and does not need to be outputted.
            if (vertexUpdateFlag) {
                Object component = updatedValues.get().getField(0, ObjectType.INSTANCE);
                context.take(ObjectRow.create(graphVertex.getId(), component));
                context.updateVertexValue(ObjectRow.create(component, false));
            }
        }
    }

    @Override
    public StructType getOutputType(GraphSchema graphSchema) {
        IType<?> idType = graphSchema.getIdType();
        return new StructType(
            new TableField("id", idType, false),
            new TableField(keyFieldName, idType, false)
        );
    }

    private void sendMessageToNeighbors(List<RowEdge> edges, Object message) {
        for (RowEdge rowEdge : edges) {
            context.sendMessage(rowEdge.getTargetId(), message);
        }
    }
}
