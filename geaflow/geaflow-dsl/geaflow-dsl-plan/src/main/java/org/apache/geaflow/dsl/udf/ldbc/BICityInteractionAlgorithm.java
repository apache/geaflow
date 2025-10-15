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

@Description(name = "bi19_interaction", description = "LDBC BI19 City Interaction Algorithm")
public class BICityInteractionAlgorithm implements AlgorithmUserFunction<Object, ObjectRow> {

    private AlgorithmRuntimeContext<Object, ObjectRow> context;

    private final int personIteration = 1;
    private final int messageIterationA = 2;
    private final int messageIterationB = 0;
    private final Long interactionMessage = -1L;
    private final Long heartbeatMessage = -2L;

    //Assert maxIterations % 3 = personIteration ensures results are collected at Person nodes.
    private final int maxIterations = 31;

    private final String personType = "Person";
    private final String postType = "Post";
    private final String commentType = "Comment";

    private final String knowsType = "knows";
    private final String hasCreatorType = "hasCreator";
    private final String replyOfType = "replyOf";
    private final String isLocatedInType = "isLocatedIn";

    private RowVertex vertexCache;
    private List<RowEdge> vertexEdgesCache = new ArrayList<>();

    private Object city1Id;
    private Object city2Id;

    @Override
    public void init(AlgorithmRuntimeContext<Object, ObjectRow> context, Object[] parameters) {
        this.context = context;
        assert parameters.length >= 2 : "Algorithm need source vid parameter.";
        city1Id = TypeCastUtil.cast(parameters[0], context.getGraphSchema().getIdType());
        assert city1Id != null : "city1Id cannot be null for algorithm.";
        city2Id = TypeCastUtil.cast(parameters[1], context.getGraphSchema().getIdType());
        assert city2Id != null : "city2Id cannot be null for algorithm.";
    }

    @Override
    public void process(RowVertex vertex, Optional<Row> updatedValues, Iterator<ObjectRow> messages) {
        if (context.getCurrentIterationId() > maxIterations) {
            return;
        }
        Long vId = (Long) vertex.getId();
        updatedValues.ifPresent(vertex::setValue);
        switch ((int) (context.getCurrentIterationId() % 3)) {
            case personIteration:
                if (vertex.getLabel().equals(personType)) {
                    Map<Long, Long> city1PersonId2DistanceMap = new HashMap<>();
                    Map<Long, Long> city2PersonId2DistanceMap = new HashMap<>();
                    if (context.getCurrentIterationId() == 1L) {
                        //Person in City 1/2 have their corresponding distance initialized to 0
                        List<RowEdge> isLocatedInEdges = loadEdges(vertex, isLocatedInType,
                            EdgeDirection.OUT);
                        for (RowEdge e : isLocatedInEdges) {
                            if (e.getTargetId().equals(city1Id)) {
                                city1PersonId2DistanceMap.put(vId, 0L);
                            }
                            if (e.getTargetId().equals(city2Id)) {
                                city2PersonId2DistanceMap.put(vId, 0L);
                            }
                        }
                    } else if (context.getCurrentIterationId() == maxIterations) {
                        decodeVertexValueAsMaps(vertex.getValue(),
                            city1PersonId2DistanceMap, city2PersonId2DistanceMap);
                        for (Long city1Person : city1PersonId2DistanceMap.keySet()) {
                            for (Long city2Person : city2PersonId2DistanceMap.keySet()) {
                                Long weight = city1PersonId2DistanceMap.get(city1Person)
                                    + city2PersonId2DistanceMap.get(city2Person);
                                context.take(ObjectRow.create(city1Person, city2Person, weight));
                            }
                        }
                        return;
                    } else {
                        decodeVertexValueAsMaps(vertex.getValue(),
                            city1PersonId2DistanceMap, city2PersonId2DistanceMap);
                    }

                    //Aggregate the total number of interactions from all the neighboring
                    // "knows" edges and temporarily store their distance map messages.
                    Map<Long, Long> personId2Interactions = new HashMap<>();
                    List<ObjectRow> distanceMapMessages = new ArrayList<>();
                    while (messages.hasNext()) {
                        ObjectRow msg = messages.next();
                        Long msgFlag = (Long) msg.getField(0, LongType.INSTANCE);
                        if (Objects.equals(msgFlag, interactionMessage)) {
                            Long personId = (Long) msg.getField(1, LongType.INSTANCE);
                            personId2Interactions.put(personId, personId2Interactions.containsKey(personId)
                                ? personId2Interactions.get(personId) + 1L : 1L);
                        } else if (msgFlag >= 0L) {
                            distanceMapMessages.add(msg);
                        }
                    }

                    for (ObjectRow msg : distanceMapMessages) {
                        Long map1Size = (Long) msg.getField(0, LongType.INSTANCE);
                        Long map2Size = (Long) msg.getField(1, LongType.INSTANCE);
                        Long senderVid = (Long) msg.getField(
                            (int) ((map1Size + map2Size + 1) * 2), LongType.INSTANCE);
                        if (personId2Interactions.containsKey(senderVid)
                            && personId2Interactions.get(senderVid) > 0L) {
                            Map<Long, Long> senderCity1PersonId2DistanceMap = new HashMap<>();
                            Map<Long, Long> senderCity2PersonId2DistanceMap = new HashMap<>();
                            decodeVertexValueAsMaps(msg,
                                senderCity1PersonId2DistanceMap, senderCity2PersonId2DistanceMap);
                            double numInteractions = personId2Interactions.get(senderVid);
                            long deltaWeight =
                                Math.max(Math.round(40 - Math.sqrt(numInteractions)), 1L);
                            for (Long city1Person : senderCity1PersonId2DistanceMap.keySet()) {
                                long newWeight =
                                    senderCity1PersonId2DistanceMap.get(city1Person) + deltaWeight;
                                if (!city1PersonId2DistanceMap.containsKey(city1Person)
                                    || newWeight < city1PersonId2DistanceMap.get(city1Person)) {
                                    city1PersonId2DistanceMap.put(city1Person, newWeight);
                                }
                            }
                            for (Long city2Person : senderCity2PersonId2DistanceMap.keySet()) {
                                long newWeight =
                                    senderCity2PersonId2DistanceMap.get(city2Person) + deltaWeight;
                                if (!city2PersonId2DistanceMap.containsKey(city2Person)
                                    || newWeight < city2PersonId2DistanceMap.get(city2Person)) {
                                    city2PersonId2DistanceMap.put(city2Person, newWeight);
                                }
                            }
                        }
                    }
                    context.updateVertexValue(encodeMapsAsVertexValue(city1PersonId2DistanceMap,
                        city2PersonId2DistanceMap, vId));

                    //Sending once own ID represent an interaction, which is then forwarded to
                    // the neighbors through 2 message nodes.
                    List<RowEdge> hasCreatorEdges = loadEdges(vertex, hasCreatorType,
                        EdgeDirection.IN);
                    for (RowEdge e : hasCreatorEdges) {
                        context.sendMessage(e.getTargetId(),
                            ObjectRow.create(interactionMessage, vId));
                    }
                }
                break;
            case messageIterationA:
                if (vertex.getLabel().equals(postType) || vertex.getLabel().equals(commentType)) {
                    List<RowEdge> replyOfEdges = loadEdges(vertex, replyOfType, EdgeDirection.BOTH);
                    while (messages.hasNext()) {
                        ObjectRow msg = messages.next();
                        for (RowEdge e : replyOfEdges) {
                            context.sendMessage(e.getTargetId(), msg);
                        }
                    }
                }
                break;
            case messageIterationB:
                if (vertex.getLabel().equals(postType) || vertex.getLabel().equals(commentType)) {
                    List<RowEdge> hasCreatorEdges = loadEdges(vertex, hasCreatorType, EdgeDirection.OUT);
                    while (messages.hasNext()) {
                        ObjectRow msg = messages.next();
                        for (RowEdge e : hasCreatorEdges) {
                            context.sendMessage(e.getTargetId(), msg);
                        }
                    }
                } else if (vertex.getLabel().equals(personType)) {
                    //The distance map message arrive at the Person node at the same time as the
                    // forwarded interaction message
                    List<RowEdge> knowsEdges = loadEdges(vertex, knowsType, EdgeDirection.BOTH);
                    for (RowEdge e : knowsEdges) {
                        context.sendMessage(e.getTargetId(), (ObjectRow) vertex.getValue());
                    }
                }
                break;
            default:
        }
        if (vertex.getLabel().equals(personType)) {
            context.sendMessage(vertex.getId(), ObjectRow.create(heartbeatMessage, 0L));
        }
    }

    @Override
    public StructType getOutputType(GraphSchema graphSchema) {
        return new StructType(
            new TableField("person1Id", LongType.INSTANCE, false),
            new TableField("person2Id", LongType.INSTANCE, false),
            new TableField("totalWeight", LongType.INSTANCE, true)
        );
    }

    private List<RowEdge> loadEdges(RowVertex vertex, String edgeLabel, EdgeDirection direction) {
        if (!vertex.equals(vertexCache)) {
            vertexEdgesCache.clear();
            vertexEdgesCache = context.loadEdges(EdgeDirection.BOTH);
            vertexCache = vertex;
        }
        List<RowEdge> results = new ArrayList<>();
        for (RowEdge e : vertexEdgesCache) {
            if (e.getLabel().equals(edgeLabel)
                && (direction == EdgeDirection.BOTH || e.getDirect() == direction)) {
                results.add(e);
            }
        }
        return results;
    }

    private static ObjectRow encodeMapsAsVertexValue(Map<Long, Long> map1, Map<Long, Long> map2,
                                                     Long... originValues) {
        int originValuesLength = originValues != null ? originValues.length : 0;
        Long[] values = new Long[(map1.keySet().size() + map2.keySet().size()) * 2 + 2 + originValuesLength];
        values[0] = (long) map1.keySet().size();
        values[1] = (long) map2.keySet().size();
        int index = 1;
        for (Long key : map1.keySet()) {
            values[index * 2] = key;
            values[index * 2 + 1] = map1.get(key);
            ++index;
        }
        for (Long key : map2.keySet()) {
            values[index * 2] = key;
            values[index * 2 + 1] = map2.get(key);
            ++index;
        }
        for (index = 0; index < originValuesLength; index++) {
            values[values.length - originValuesLength + index] = originValues[index];
        }
        return ObjectRow.create((Object[]) values);
    }

    private static void decodeVertexValueAsMaps(Row objectRow, Map<Long, Long> map1, Map<Long, Long> map2) {
        long size1 = (long) objectRow.getField(0, LongType.INSTANCE);
        long size2 = (long) objectRow.getField(1, LongType.INSTANCE);
        int i = 1;
        for (int j = 0; j < size1; j++) {
            Long key = (Long) objectRow.getField(i * 2, LongType.INSTANCE);
            Long value = (Long) objectRow.getField(i * 2 + 1, LongType.INSTANCE);
            map1.put(key, value);
            ++i;
        }
        for (int j = 0; j < size2; j++) {
            Long key = (Long) objectRow.getField(i * 2, LongType.INSTANCE);
            Long value = (Long) objectRow.getField(i * 2 + 1, LongType.INSTANCE);
            map2.put(key, value);
            ++i;
        }
    }

    @Override
    public void finish(RowVertex vertex, Optional<Row> newValue) {
    }
}
