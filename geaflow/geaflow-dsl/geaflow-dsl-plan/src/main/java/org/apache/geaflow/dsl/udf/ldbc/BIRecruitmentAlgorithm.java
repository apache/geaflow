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

package org.apache.geaflow.dsl.udf.ldbc;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
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

@Description(name = "bi20_recruitment", description = "LDBC BI20 Recruitment Algorithm")
public class BIRecruitmentAlgorithm implements AlgorithmUserFunction<Object, ObjectRow> {

    private AlgorithmRuntimeContext<Object, ObjectRow> context;
    private Object sourceVertexId;
    private final int maxIteration = 30;
    private final String knowsType = "knows";
    private final String studyAtType = "studyAt";
    private final String personType = "Person";

    @Override
    public void init(AlgorithmRuntimeContext<Object, ObjectRow> context, Object[] parameters) {
        this.context = context;
        assert parameters.length >= 1 : "SSSP algorithm need source vid parameter.";
        sourceVertexId = TypeCastUtil.cast(parameters[0], context.getGraphSchema().getIdType());
        assert sourceVertexId != null : "Source vid cannot be null for SSSP.";
    }

    @Override
    public void process(RowVertex vertex, Optional<Row> updatedValues, Iterator<ObjectRow> messages) {
        if (!vertex.getLabel().equals(personType)) {
            return;
        }
        Object vId = vertex.getId();
        updatedValues.ifPresent(vertex::setValue);
        List<RowEdge> outEdges = context.loadEdges(EdgeDirection.BOTH);
        List<Object> sendMsgTargetIds = new ArrayList<>();
        Map<Object, Long> university2ClassYear = new HashMap<>();
        for (RowEdge edge : outEdges) {
            if (edge.getLabel().equals(knowsType)) {
                sendMsgTargetIds.add(edge.getTargetId());
            } else if (edge.getLabel().equals(studyAtType)) {
                university2ClassYear.put(edge.getTargetId(),
                    (Long) edge.getValue().getField(0, LongType.INSTANCE));
            }
        }

        Long currentDistance;
        if (context.getCurrentIterationId() == 1L) {
            if (Objects.equals(vId, sourceVertexId)) {
                currentDistance = 0L;
            } else {
                currentDistance = Long.MAX_VALUE;
            }
        } else if (context.getCurrentIterationId() <= maxIteration) {
            currentDistance = (Long) vertex.getValue().getField(0, LongType.INSTANCE);
            //Msg schema: Person.id BIGINT, distance BIGINT, University.id BIGINT, classYear BIGINT
            while (messages.hasNext()) {
                ObjectRow msg = messages.next();
                Long inputDistance = (Long) msg.getField(1, LongType.INSTANCE);
                Long universityId = (Long) msg.getField(2, LongType.INSTANCE);
                Long classYear = (Long) msg.getField(3, LongType.INSTANCE);
                Long newDistance = Long.MAX_VALUE;
                if (inputDistance != Long.MAX_VALUE && university2ClassYear.containsKey(universityId)) {
                    newDistance = inputDistance + 1L
                        + Math.abs(university2ClassYear.get(universityId) - classYear);
                }
                if (newDistance < currentDistance) {
                    currentDistance = newDistance;
                }
            }
        } else {
            currentDistance = (long) vertex.getValue().getField(0, LongType.INSTANCE);
            if (!vId.equals(sourceVertexId)) {
                context.take(ObjectRow.create(TypeCastUtil.cast(vId, LongType.INSTANCE), currentDistance));
            }
            return;
        }
        context.updateVertexValue(ObjectRow.create(currentDistance));
        //Send active heartbeat message
        context.sendMessage(vId, ObjectRow.create(new Object[]{0L, Long.MAX_VALUE, 0L, 0L}));
        //Scatter
        //Msg schema: Person.id BIGINT, distance BIGINT, University.id BIGINT, classYear BIGINT
        for (Object targetId : sendMsgTargetIds) {
            for (Entry<Object, Long> universityMsg : university2ClassYear.entrySet()) {
                context.sendMessage(targetId, ObjectRow.create(
                    vId,
                    currentDistance,
                    universityMsg.getKey(),
                    universityMsg.getValue()
                ));
            }
        }
    }

    @Override
    public StructType getOutputType(GraphSchema graphSchema) {
        return new StructType(
            new TableField("id", LongType.INSTANCE, false),
            new TableField("distance", LongType.INSTANCE, false)
        );
    }

    @Override
    public void finish(RowVertex vertex, Optional<Row> newValue) {
    }
}
