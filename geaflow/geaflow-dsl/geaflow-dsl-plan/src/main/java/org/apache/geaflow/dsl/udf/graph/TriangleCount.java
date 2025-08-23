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

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
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
import org.apache.geaflow.model.graph.edge.EdgeDirection;

@Description(name = "triangle_count", description = "built-in udga for Triangle Count.")
public class TriangleCount implements AlgorithmUserFunction<Object, ObjectRow> {

    private AlgorithmRuntimeContext<Object, ObjectRow> context;

    private final int maxIteration = 2;

    private String vertexType = null;

    private final Set<Long> excludeSet = Sets.newHashSet();

    @Override
    public void init(AlgorithmRuntimeContext<Object, ObjectRow> context, Object[] params) {
        this.context = context;
        if (params.length >= 1) {
            assert params[0] instanceof String : "Vertex type parameter should be string.";
            vertexType = (String) params[0];
        }
        assert params.length <= 1 : "Maximum parameter limit exceeded.";
    }

    @Override
    public void process(RowVertex vertex, Optional<Row> updatedValues, Iterator<ObjectRow> messages) {
        updatedValues.ifPresent(vertex::setValue);

        if (context.getCurrentIterationId() == 1L) {
            if (Objects.nonNull(vertexType) && !vertexType.equals(vertex.getLabel())) {
                excludeSet.add((Long) vertex.getId());
                return;
            }

            List<RowEdge> rowEdges = context.loadEdges(EdgeDirection.BOTH);
            List<Object> neighborInfo = Lists.newArrayList();
            neighborInfo.add((long) rowEdges.size());
            for (RowEdge rowEdge : rowEdges) {
                neighborInfo.add(rowEdge.getTargetId());
            }
            ObjectRow msg = ObjectRow.create(neighborInfo.toArray());
            for (int i = 1; i < neighborInfo.size(); i++) {
                context.sendMessage(neighborInfo.get(i), msg);
            }
            context.sendMessage(vertex.getId(), ObjectRow.create(0L));
            context.updateVertexValue(msg);
        } else if (context.getCurrentIterationId() <= maxIteration) {
            if (Objects.nonNull(vertexType) && !vertexType.equals(vertex.getLabel())) {
                return;
            }
            long count = 0;
            Set<Long> sourceSet = row2Set(vertex.getValue());
            while (messages.hasNext()) {
                ObjectRow msg = messages.next();
                Set<Long> targetSet = row2Set(msg);
                targetSet.retainAll(sourceSet);
                count += targetSet.size();
            }
            context.take(ObjectRow.create(vertex.getId(), count / 2));
        }
    }

    @Override
    public void finish(RowVertex graphVertex, Optional<Row> updatedValues) {

    }

    @Override
    public StructType getOutputType(GraphSchema graphSchema) {
        return new StructType(
            new TableField("id", graphSchema.getIdType(), false),
            new TableField("count", LongType.INSTANCE, false)
        );
    }

    private Set<Long> row2Set(Row row) {
        long len = (long) row.getField(0, LongType.INSTANCE);
        Object[] ids = new Object[(int) len];
        for (int i = 0; i < len; i++) {
            ids[i] = row.getField(i + 1, LongType.INSTANCE);
        }
        Set<Long> set = Sets.newHashSet();
        for (Object id : ids) {
            if (!excludeSet.contains((long) id)) {
                set.add((long) id);
            }
        }
        return set;
    }
}
