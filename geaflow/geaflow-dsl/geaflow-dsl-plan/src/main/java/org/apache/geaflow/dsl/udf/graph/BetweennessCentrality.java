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
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
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
import org.apache.geaflow.dsl.common.util.TypeCastUtil;
import org.apache.geaflow.model.graph.edge.EdgeDirection;

@Description(name = "betweenness_centrality", description = "built-in udga for BetweennessCentrality")
public class BetweennessCentrality implements AlgorithmUserFunction<Object, Object[]> {

    private AlgorithmRuntimeContext<Object, Object[]> context;
    private int maxDiameter = 10;

    @Override
    public void init(AlgorithmRuntimeContext<Object, Object[]> context, Object[] parameters) {
        this.context = context;
        if (parameters.length > 1) {
            throw new IllegalArgumentException(
                "Only support zero or one arguments, usage: betweenness([maxDiameter])");
        }
        if (parameters.length == 1) {
            maxDiameter = Integer.parseInt(String.valueOf(parameters[0]));
        }
    }

    @Override
    public void process(RowVertex vertex, Optional<Row> updatedValues, Iterator<Object[]> messages) {
        long iter = context.getCurrentIterationId();
        double score;
        Map<Object, double[]> srcStateMap;

        if (iter == 1L) {
            score = 0.0;
            srcStateMap = new HashMap<>();
            // Each vertex is initially its own source with distance 0 and sigma 1
            srcStateMap.put(vertex.getId(), new double[]{0.0, 1.0, 0.0});
            broadcastForward(vertex.getId(), 0L, 1L);
            context.updateVertexValue(ObjectRow.create(score, srcStateMap));
            return;
        }

        if (updatedValues.isPresent()) {
            Row row = updatedValues.get();
            score = (double) row.getField(0, DoubleType.INSTANCE);
            srcStateMap = (Map<Object, double[]>) row.getField(1, null);
        } else {
            score = 0.0;
            srcStateMap = new HashMap<>();
        }

        if (iter <= maxDiameter + 1) {
            // Forward phase (iterations 2 to maxDiameter + 1)
            Map<Object, List<long[]>> incoming = new HashMap<>();
            while (messages.hasNext()) {
                Object[] msg = messages.next();
                if ("F".equals(msg[0])) {
                    Object srcId = msg[1];
                    long mDist = (long) msg[2];
                    long mSigma = (long) msg[3];
                    incoming.computeIfAbsent(srcId, k -> new ArrayList<>()).add(new long[]{mDist, mSigma});
                }
            }

            for (Map.Entry<Object, List<long[]>> entry : incoming.entrySet()) {
                Object srcId = entry.getKey();
                long minDist = Long.MAX_VALUE;
                for (long[] m : entry.getValue()) {
                    if (m[0] < minDist) {
                        minDist = m[0];
                    }
                }

                long sumSigma = 0;
                for (long[] m : entry.getValue()) {
                    if (m[0] == minDist) {
                        sumSigma += m[1];
                    }
                }

                long newDist = minDist + 1;
                double[] state = srcStateMap.get(srcId);
                if (state == null) {
                    srcStateMap.put(srcId, new double[]{newDist, sumSigma, 0.0});
                    broadcastForward(srcId, newDist, sumSigma);
                } else {
                    if (newDist < (long) state[0]) {
                        state[0] = newDist;
                        state[1] = sumSigma;
                        broadcastForward(srcId, newDist, sumSigma);
                    } else if (newDist == (long) state[0]) {
                        state[1] += sumSigma;
                        broadcastForward(srcId, newDist, sumSigma);
                    }
                }
            }
            context.updateVertexValue(ObjectRow.create(score, srcStateMap));

        } else {
            // Backward phase (iterations maxDiameter + 2 to 2 * maxDiameter + 2)
            while (messages.hasNext()) {
                Object[] msg = messages.next();
                if ("B".equals(msg[0])) {
                    Object srcId = msg[1];
                    double wSigma = (double) msg[2];
                    double wDelta = (double) msg[3];

                    double[] state = srcStateMap.get(srcId);
                    if (state != null) {
                        long myDist = (long) state[0];
                        if (myDist == 2 * maxDiameter + 2 - iter) {
                            double mySigma = state[1];
                            if (mySigma > 0 && wSigma > 0) {
                                state[2] += (mySigma / wSigma) * (1.0 + wDelta);
                            }
                        }
                    }
                }
            }

            long targetDist = 2 * maxDiameter + 2 - iter;
            for (Map.Entry<Object, double[]> entry : srcStateMap.entrySet()) {
                Object srcId = entry.getKey();
                double[] state = entry.getValue();
                long myDist = (long) state[0];
                if (myDist == targetDist && myDist > 0 && !srcId.equals(vertex.getId())) {
                    broadcastBackward(srcId, state[1], state[2]);
                }
            }

            if (iter == 2 * maxDiameter + 2) {
                for (Map.Entry<Object, double[]> entry : srcStateMap.entrySet()) {
                    Object srcId = entry.getKey();
                    if (!srcId.equals(vertex.getId())) {
                        score += entry.getValue()[2];
                    }
                }
            }
            context.updateVertexValue(ObjectRow.create(score, srcStateMap));
        }
    }

    @Override
    public void finish(RowVertex vertex, Optional<Row> newValue) {
        double score = 0.0;
        if (newValue.isPresent()) {
            score = (double) newValue.get().getField(0, DoubleType.INSTANCE);
        }
        context.take(ObjectRow.create(vertex.getId(), score));
    }

    @Override
    public StructType getOutputType(GraphSchema graphSchema) {
        return new StructType(
            new TableField("id", graphSchema.getIdType(), false),
            new TableField("score", DoubleType.INSTANCE, false)
        );
    }

    private void broadcastForward(Object srcId, long dist, long sigma) {
        Object[] msg = new Object[]{"F", srcId, dist, sigma};
        List<RowEdge> edges = context.loadEdges(EdgeDirection.OUT);
        if (edges != null) {
            Set<Object> targetIds = new HashSet<>();
            for (RowEdge e : edges) {
                targetIds.add(e.getTargetId());
            }
            for (Object targetId : targetIds) {
                context.sendMessage(targetId, msg);
            }
        }
    }

    private void broadcastBackward(Object srcId, double sigma, double delta) {
        Object[] msg = new Object[]{"B", srcId, sigma, delta};
        List<RowEdge> edges = context.loadEdges(EdgeDirection.IN);
        if (edges != null) {
            Set<Object> targetIds = new HashSet<>();
            for (RowEdge e : edges) {
                targetIds.add(e.getTargetId());
            }
            for (Object targetId : targetIds) {
                context.sendMessage(targetId, msg);
            }
        }
    }
}
