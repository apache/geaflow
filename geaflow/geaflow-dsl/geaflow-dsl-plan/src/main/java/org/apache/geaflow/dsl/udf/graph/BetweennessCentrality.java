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

/**
 * Built-in UDGA computing Betweenness Centrality via a vertex-centric
 * adaptation of Brandes' algorithm.
 *
 * <p>All vertices act as sources simultaneously. Each vertex keeps per-source
 * state {@code st[source] = [dist, sigma, delta, childCount, respCount,
 * recvChild, pendingSum, forwarded, backwardDone]} plus its per-source
 * predecessor list.</p>
 *
 * <p>Phase 1 (forward): a BFS from every source counts the number of shortest
 * paths ({@code sigma}) reaching each vertex and records predecessors. Every
 * forwarded neighbour replies ACK (it is a child) or NACK (it is not), so a
 * vertex learns how many children it has without a global barrier.</p>
 *
 * <p>Phase 2 (backward): once a vertex has heard from all children for a source
 * it finalizes its dependency {@code delta = sigma * sum((1 + delta_child) /
 * sigma_child)}, adds it to its betweenness score, and pushes the contribution
 * to its predecessors.</p>
 *
 * <p>Directed, unnormalized betweenness (endpoints excluded). Complexity is
 * O(V * E) time; space is O(V^2) because per-source state is kept on every
 * vertex, so this suits small / medium graphs.</p>
 */
@Description(name = "betweenness_centrality", description = "built-in udga for BetweennessCentrality")
public class BetweennessCentrality implements AlgorithmUserFunction<Object, List<Object>> {

    // Message tags.
    private static final int FWD = 0;
    private static final int ACK = 1;
    private static final int NACK = 2;
    private static final int BWD = 3;

    // Per-source state array indices.
    private static final int DIST = 0;
    private static final int SIGMA = 1;
    private static final int DELTA = 2;
    private static final int CHILD_COUNT = 3;
    private static final int RESP_COUNT = 4;
    private static final int RECV_CHILD = 5;
    private static final int PENDING_SUM = 6;
    private static final int FORWARDED = 7;
    private static final int BACKWARD_DONE = 8;
    private static final int STATE_LEN = 9;

    private AlgorithmRuntimeContext<Object, List<Object>> context;

    @Override
    public void init(AlgorithmRuntimeContext<Object, List<Object>> context, Object[] params) {
        this.context = context;
        if (params.length > 0) {
            throw new IllegalArgumentException(
                "The betweenness_centrality algorithm takes no arguments, usage: betweenness_centrality()");
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public void process(RowVertex vertex, Optional<Row> updatedValues, Iterator<List<Object>> messages) {
        Object vid = vertex.getId();
        List<RowEdge> outEdges = context.loadEdges(EdgeDirection.OUT);
        int outDegree = outEdges.size();

        // Iteration 1: every vertex initializes itself as a source and scatters.
        if (context.getCurrentIterationId() == 1L) {
            double[] arr = new double[STATE_LEN];
            arr[DIST] = 0;
            arr[SIGMA] = 1;
            arr[FORWARDED] = 1;
            Map<String, Object> state = newState();
            Map<Object, double[]> st = (Map<Object, double[]>) state.get("st");
            st.put(vid, arr);
            Map<Object, List<Object>> preds = (Map<Object, List<Object>>) state.get("preds");
            preds.put(vid, new ArrayList<>());
            for (RowEdge edge : outEdges) {
                context.sendMessage(edge.getTargetId(), forwardMsg(vid, vid, 0L, 1.0));
            }
            context.updateVertexValue(ObjectRow.create(state));
            return;
        }

        Map<String, Object> state = (Map<String, Object>) updatedValues.get().getField(0, null);
        double bc = (Double) state.get("bc");
        final Map<Object, double[]> st = (Map<Object, double[]>) state.get("st");

        // Group incoming forward messages by source; apply ack/nack/backward directly.
        Map<Object, List<List<Object>>> forwardBySource = new HashMap<>();
        while (messages.hasNext()) {
            List<Object> msg = messages.next();
            int tag = ((Number) msg.get(0)).intValue();
            Object source = msg.get(1);
            if (tag == FWD) {
                forwardBySource.computeIfAbsent(source, k -> new ArrayList<>()).add(msg);
            } else if (tag == ACK) {
                double[] arr = st.get(source);
                if (arr != null) {
                    arr[CHILD_COUNT]++;
                    arr[RESP_COUNT]++;
                }
            } else if (tag == NACK) {
                double[] arr = st.get(source);
                if (arr != null) {
                    arr[RESP_COUNT]++;
                }
            } else if (tag == BWD) {
                double value = ((Number) msg.get(2)).doubleValue();
                double[] arr = st.get(source);
                if (arr != null) {
                    arr[PENDING_SUM] += value;
                    arr[RECV_CHILD]++;
                }
            }
        }

        // Handle forward messages: update sigma / predecessors, reply ack/nack, scatter once.
        final Map<Object, List<Object>> preds = (Map<Object, List<Object>>) state.get("preds");
        for (Map.Entry<Object, List<List<Object>>> entry : forwardBySource.entrySet()) {
            Object source = entry.getKey();
            for (List<Object> msg : entry.getValue()) {
                Object sender = msg.get(2);
                int senderDist = ((Number) msg.get(3)).intValue();
                double senderSigma = ((Number) msg.get(4)).doubleValue();
                int candidate = senderDist + 1;
                double[] arr = st.get(source);
                if (arr == null) {
                    arr = new double[STATE_LEN];
                    arr[DIST] = candidate;
                    arr[SIGMA] = 0;
                    st.put(source, arr);
                    preds.put(source, new ArrayList<>());
                }
                if ((int) arr[DIST] == candidate) {
                    arr[SIGMA] += senderSigma;
                    preds.get(source).add(sender);
                    context.sendMessage(sender, tagSourceMsg(ACK, source));
                } else {
                    context.sendMessage(sender, tagSourceMsg(NACK, source));
                }
            }
            double[] arr = st.get(source);
            if (arr[FORWARDED] == 0) {
                arr[FORWARDED] = 1;
                long dist = (long) arr[DIST];
                double sigma = arr[SIGMA];
                for (RowEdge edge : outEdges) {
                    context.sendMessage(edge.getTargetId(), forwardMsg(source, vid, dist, sigma));
                }
            }
        }

        // Backward accumulation: finalize any source whose children have all reported.
        for (Object source : new ArrayList<>(st.keySet())) {
            double[] arr = st.get(source);
            if (arr[BACKWARD_DONE] == 1) {
                continue;
            }
            boolean forwarded = arr[FORWARDED] == 1;
            boolean childCountFinal = forwarded && arr[RESP_COUNT] >= outDegree;
            if (childCountFinal && arr[RECV_CHILD] >= arr[CHILD_COUNT]) {
                double sigma = arr[SIGMA];
                double delta = sigma * arr[PENDING_SUM];
                arr[DELTA] = delta;
                if (!source.equals(vid)) {
                    bc += delta;
                }
                arr[BACKWARD_DONE] = 1;
                double value = (1.0 + delta) / sigma;
                for (Object pred : preds.get(source)) {
                    context.sendMessage(pred, backwardMsg(source, value));
                }
            }
        }

        state.put("bc", bc);
        context.updateVertexValue(ObjectRow.create(state));
    }

    @Override
    @SuppressWarnings("unchecked")
    public void finish(RowVertex vertex, Optional<Row> updatedValues) {
        double bc = 0.0;
        if (updatedValues.isPresent()) {
            Map<String, Object> state = (Map<String, Object>) updatedValues.get().getField(0, null);
            if (state != null && state.get("bc") != null) {
                bc = (Double) state.get("bc");
            }
        }
        context.take(ObjectRow.create(vertex.getId(), bc));
    }

    @Override
    public StructType getOutputType(GraphSchema graphSchema) {
        return new StructType(
            new TableField("id", graphSchema.getIdType(), false),
            new TableField("betweenness", DoubleType.INSTANCE, false)
        );
    }

    private static Map<String, Object> newState() {
        Map<String, Object> state = new HashMap<>();
        state.put("st", new HashMap<Object, double[]>());
        state.put("preds", new HashMap<Object, List<Object>>());
        state.put("bc", 0.0);
        return state;
    }

    private static List<Object> forwardMsg(Object source, Object sender, long dist, double sigma) {
        List<Object> msg = new ArrayList<>(5);
        msg.add(FWD);
        msg.add(source);
        msg.add(sender);
        msg.add(dist);
        msg.add(sigma);
        return msg;
    }

    private static List<Object> tagSourceMsg(int tag, Object source) {
        List<Object> msg = new ArrayList<>(2);
        msg.add(tag);
        msg.add(source);
        return msg;
    }

    private static List<Object> backwardMsg(Object source, double value) {
        List<Object> msg = new ArrayList<>(3);
        msg.add(BWD);
        msg.add(source);
        msg.add(value);
        return msg;
    }
}
