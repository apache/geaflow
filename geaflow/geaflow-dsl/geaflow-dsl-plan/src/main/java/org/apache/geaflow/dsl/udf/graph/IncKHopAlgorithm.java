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
import org.apache.geaflow.common.config.ConfigHelper;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Description(name = "inc_khop", description = "built-in incur udf for KHop")
public class IncKHopAlgorithm implements AlgorithmUserFunction<Object, IntTreePathMessage> {

    private static final Logger LOGGER = LoggerFactory.getLogger(IncKHopAlgorithm.class);

    private static final String SKIP_OUTPUT = "khop.skip.output";

    private AlgorithmRuntimeContext<Object, IntTreePathMessage> context;
    protected long maxIterNum;
    private boolean skipOutput = false;
    protected Set<Object> intMessageSet = new HashSet<>();
    protected Map<Object, List<RowEdge>> outEdgeMap = new HashMap<>();
    protected Map<Object, List<RowEdge>> inEdgeMap = new HashMap<>();
    private Map<Object, List<IntTreePathMessage>> stashInPathMessageMap = new HashMap<>();
    private Map<Object, List<IntTreePathMessage>> stashOutPathMessageMap = new HashMap<>();

    @Override
    public void init(AlgorithmRuntimeContext<Object, IntTreePathMessage> context, Object[] parameters) {
        this.context = context;
        this.maxIterNum = Integer.parseInt(String.valueOf(parameters[0])) + 2;
        this.skipOutput = ConfigHelper.getBooleanOrDefault(context.getConfig().getConfigMap(), SKIP_OUTPUT, false);
    }

    @Override
    public void process(RowVertex vertex, Optional<Row> updatedValues, Iterator<IntTreePathMessage> messageIterator) {
        long currentIterationId = context.getCurrentIterationId();
        if (currentIterationId == 1) {
            List<RowEdge> dynamicOutEdges = context.loadDynamicEdges(EdgeDirection.OUT);
            if (dynamicOutEdges.isEmpty()) {
                return;
            }
            IntTreePathMessage currentVPathMessage = new IntTreePathMessage(vertex.getId());
            sendMessage(dynamicOutEdges, currentVPathMessage);
            sendMessage(vertex.getId());
        } else if (currentIterationId < maxIterNum) {
            List<RowEdge> staticOutEdges = getStaticOutEdges(vertex.getId());
            List<RowEdge> staticInEdges = getStaticInEdges(vertex.getId());
            List<RowEdge> tmpEdges = context.loadDynamicEdges(EdgeDirection.BOTH);
            List<RowEdge> dynamicInEdges = new ArrayList<>();
            List<RowEdge> dynamicOutEdges = new ArrayList<>();
            if (tmpEdges != null) {
                for (RowEdge edge : tmpEdges) {
                    if (edge.getDirect() == EdgeDirection.OUT) {
                        dynamicOutEdges.add(edge);
                    } else {
                        dynamicInEdges.add(edge);
                    }
                }
            }

            IntTreePathMessage sendOutPathMessage = null;
            IntTreePathMessage sendInPathMessage = null;
            List<IntTreePathMessage> outMessages = new ArrayList<>();
            List<IntTreePathMessage> inMessages = new ArrayList<>();
            while (messageIterator.hasNext()) {
                IntTreePathMessage pathMsg = (IntTreePathMessage) messageIterator.next();
                if (pathMsg.getCurrentVertexId() == null) {
                    continue;
                }
                int depth = pathMsg.getPathLength();
                if (depth == currentIterationId - 1) {
                    // out dir traversal message
                    outMessages.add(pathMsg);
                } else if (depth < currentIterationId - 1) {
                    // in dir traversal message
                    inMessages.add(pathMsg);
                }
            }
            if (!outMessages.isEmpty()) {
                IntTreePathMessage[] parent = new IntTreePathMessage[outMessages.size()];
                for (int i = 0; i < outMessages.size(); i++) {
                    parent[i] = outMessages.get(i);
                }
                sendOutPathMessage = new IntTreePathMessage(parent, vertex.getId());
            }
            if (!inMessages.isEmpty()) {
                if (currentIterationId == 2) {
                    throw new RuntimeException("iter 2 should not have in path message");
                }
                IntTreePathMessage[] parent = new IntTreePathMessage[inMessages.size()];
                for (int i = 0; i < inMessages.size(); i++) {
                    parent[i] = inMessages.get(i);
                }
                sendInPathMessage = new IntTreePathMessage(parent, vertex.getId());
            } else {
                if (currentIterationId == 2) {
                    sendInPathMessage = new IntTreePathMessage(null, vertex.getId());
                }
            }

            if (currentIterationId < maxIterNum - 1) {
                if (!stashOutPathMessageMap.containsKey(vertex.getId())) {
                    stashOutPathMessageMap.put(vertex.getId(), new ArrayList<>());
                }
                List<IntTreePathMessage> stashOutPathMessages = stashOutPathMessageMap.get(vertex.getId());
                if (sendOutPathMessage != null) {
                    sendMessage(staticOutEdges, sendOutPathMessage);
                    sendMessage(dynamicOutEdges, sendOutPathMessage);
                    //合并消息树开启时，只在无消息发出时，保存当前路径
                    if (staticOutEdges.isEmpty() && dynamicOutEdges.isEmpty()) {
                        stashOutPathMessages.add(sendOutPathMessage);
                    }
                }

                if (!stashInPathMessageMap.containsKey(vertex.getId())) {
                    stashInPathMessageMap.put(vertex.getId(), new ArrayList<>());
                }
                List<IntTreePathMessage> stashInPathMessages = stashInPathMessageMap.get(vertex.getId());
                if (sendInPathMessage != null) {
                    sendMessage(staticInEdges, sendInPathMessage);
                    sendMessage(dynamicInEdges, sendInPathMessage);
                    //合并消息树开启时，只在无消息发出时，保存当前路径
                    if (staticInEdges.isEmpty() && dynamicInEdges.isEmpty()) {
                        stashInPathMessages.add(sendInPathMessage);
                    }
                }
                // 激活自己
                sendMessage(vertex.getId());
            } else if (currentIterationId == maxIterNum - 1) {
                // tree path is reversed.
                Map<Object, IntTreePathMessage> head2OutMixMsg = new HashMap<>();
                Map<Object, IntTreePathMessage> head2InMixMsg = new HashMap<>();
                if (sendOutPathMessage != null) {
                    Map<Object, List<Object[]>> pathMap = sendOutPathMessage.generatePathMap();
                    Iterator<Object> keys = pathMap.keySet().iterator();
                    while (keys.hasNext()) {
                        Object head = keys.next();
                        IntTreePathMessage outMsg = head2OutMixMsg.get(head);
                        if (outMsg == null) {
                            outMsg = new IntTreePathMessage.IntTreePathMessageWrapper(null, 1);
                            head2OutMixMsg.put(head, outMsg);
                        }
                        Iterator<Object[]> itr = pathMap.get(head).iterator();
                        while (itr.hasNext()) {
                            Object[] tmp = itr.next();
                            outMsg.addPath(tmp);
                        }
                    }
                }

                if (stashOutPathMessageMap.containsKey(vertex.getId())) {
                    List<IntTreePathMessage> stashOutPathMessages = stashOutPathMessageMap.get(vertex.getId());
                    for (IntTreePathMessage pathMessage : stashOutPathMessages) {
                        Map<Object, List<Object[]>> pathMap = pathMessage.generatePathMap();
                        Iterator<Object> keys = pathMap.keySet().iterator();
                        while (keys.hasNext()) {
                            Object head = keys.next();
                            IntTreePathMessage outMsg = head2OutMixMsg.get(head);
                            if (outMsg == null) {
                                outMsg = new IntTreePathMessage.IntTreePathMessageWrapper(null, 1);
                                head2OutMixMsg.put(head, outMsg);
                            }
                            Iterator<Object[]> itr = pathMap.get(head).iterator();
                            while (itr.hasNext()) {
                                Object[] tmp = itr.next();
                                outMsg.addPath(tmp);
                            }
                        }
                    }
                }

                if (sendInPathMessage != null) {
                    Map<Object, List<Object[]>> pathMap = sendInPathMessage.generatePathMap();
                    Iterator<Object> keys = pathMap.keySet().iterator();
                    while (keys.hasNext()) {
                        Object head = keys.next();
                        IntTreePathMessage inMsg = head2InMixMsg.get(head);
                        if (inMsg == null) {
                            inMsg = new IntTreePathMessage.IntTreePathMessageWrapper(null, 0);
                            head2InMixMsg.put(head, inMsg);
                        }
                        Iterator<Object[]> itr = pathMap.get(head).iterator();
                        while (itr.hasNext()) {
                            Object[] tmp = itr.next();
                            inMsg.addPath(tmp);
                        }
                    }
                }

                if (stashInPathMessageMap.containsKey(vertex.getId())) {
                    List<IntTreePathMessage> stashInPathMessages = stashInPathMessageMap.get(vertex.getId());
                    for (IntTreePathMessage pathMessage : stashInPathMessages) {
                        Map<Object, List<Object[]>> pathMap = pathMessage.generatePathMap();
                        Iterator<Object> keys = pathMap.keySet().iterator();
                        while (keys.hasNext()) {
                            Object head = keys.next();
                            IntTreePathMessage inMsg = head2InMixMsg.get(head);
                            if (inMsg == null) {
                                inMsg = new IntTreePathMessage.IntTreePathMessageWrapper(null, 0);
                                head2InMixMsg.put(head, inMsg);
                            }
                            Iterator<Object[]> itr = pathMap.get(head).iterator();
                            while (itr.hasNext()) {
                                Object[] tmp = itr.next();
                                inMsg.addPath(tmp);
                            }
                        }
                    }
                }

                Iterator<Object> keys = head2OutMixMsg.keySet().iterator();
                while (keys.hasNext()) {
                    Object head = keys.next();
                    context.sendMessage(head, head2OutMixMsg.get(head));
                }
                keys = head2InMixMsg.keySet().iterator();
                while (keys.hasNext()) {
                    Object head = keys.next();
                    context.sendMessage(head, head2InMixMsg.get(head));
                }
            }
        } else {
            IntTreePathMessage sendOutPathMessage = new IntTreePathMessage();
            IntTreePathMessage sendInPathMessage = new IntTreePathMessage();
            while (messageIterator.hasNext()) {
                IntTreePathMessage.IntTreePathMessageWrapper pathMsg =
                    (IntTreePathMessage.IntTreePathMessageWrapper) messageIterator.next();
                if (pathMsg.getTag() == 0) {
                    // in path
                    sendInPathMessage.merge(pathMsg);
                } else {
                    sendOutPathMessage.merge(pathMsg);
                }
            }
            if (!skipOutput) {
                constructResult(sendOutPathMessage, sendInPathMessage, (int) maxIterNum - 1);
            }
        }
    }

    @Override
    public StructType getOutputType(GraphSchema graphSchema) {
        return new StructType(
            new TableField("ret", StringType.INSTANCE, false)
        );
    }

    @Override
    public void finishIteration(long iterationId) {
        this.intMessageSet.clear();
    }

    @Override
    public void finish(RowVertex graphVertex, Optional<Row> updatedValues) {
    }

    @Override
    public void finish() {
        this.intMessageSet.clear();
        this.outEdgeMap.clear();
        this.inEdgeMap.clear();
        this.stashInPathMessageMap.clear();
        this.stashOutPathMessageMap.clear();
    }

    protected void sendMessage(List<RowEdge> edges) {
        for (RowEdge edge : edges) {
            if (!intMessageSet.contains(edge.getTargetId())) {
                intMessageSet.add(edge.getTargetId());
                context.sendMessage(edge.getTargetId(), new IntTreePathMessage());
            }
        }
    }

    protected void sendMessage(Object targetId) {
        if (!intMessageSet.contains(targetId)) {
            intMessageSet.add(targetId);
            context.sendMessage(targetId, new IntTreePathMessage());
        }
    }

    protected void sendMessage(List<RowEdge> edges, IntTreePathMessage pathMessage) {
        for (RowEdge edge : edges) {
            context.sendMessage(edge.getTargetId(), pathMessage);
        }
    }

    protected List<RowEdge> getStaticInEdges(Object vid) {
        List<RowEdge> staticInEdges = inEdgeMap.get(vid);
        if (staticInEdges == null) {
            staticInEdges = this.context.loadStaticEdges(EdgeDirection.IN);
            inEdgeMap.put(vid, staticInEdges);
            return staticInEdges;
        } else {
            return staticInEdges;
        }
    }

    protected List<RowEdge> getStaticOutEdges(Object vid) {
        List<RowEdge> staticOutEdges = outEdgeMap.get(vid);
        if (staticOutEdges == null) {
            staticOutEdges = this.context.loadStaticEdges(EdgeDirection.OUT);
            outEdgeMap.put(vid, staticOutEdges);
            return staticOutEdges;
        } else {
            return staticOutEdges;
        }
    }

    private void constructResult(IntTreePathMessage outMessage, IntTreePathMessage inMessage, int expectedLength) {

        // this path is begin with start central vertex
        Iterator<Object[]> outPathIterator = outMessage.getPaths();
        Map<Integer, List<Object[]>> outPathMap = new HashMap<>();
        while (outPathIterator.hasNext()) {
            Object[] currentPath = outPathIterator.next();
            if (!outPathMap.containsKey(currentPath.length)) {
                outPathMap.put(currentPath.length, new ArrayList<>());
            }
            outPathMap.get(currentPath.length).add(currentPath);
        }
        // this iterator is end with central vertex
        Iterator<Object[]> inPathIterator = inMessage.getPaths();
        Map<Integer, List<Object[]>> inPathMap = new HashMap<>();
        while (inPathIterator.hasNext()) {
            Object[] currentPath = inPathIterator.next();
            if (!inPathMap.containsKey(currentPath.length)) {
                inPathMap.put(currentPath.length, new ArrayList<>());
            }
            inPathMap.get(currentPath.length).add(currentPath);
        }
        for (int outLength = expectedLength; outLength > 0; outLength--) {
            if (outLength == expectedLength) {
                List<Object[]> outPaths = outPathMap.get(outLength);
                if (outPaths == null) {
                    continue;
                }
                for (Object[] outPath : outPaths) {
                    String pathStr = convertToPath(outPath, outLength, null, 0);
                    if (!skipOutput) {
                        context.take(ObjectRow.create(pathStr));
                    }
                }
            } else {
                int inLength = expectedLength - outLength + 1;
                Set<String> inPaths = getPaths(inPathMap, inLength, expectedLength, 0);
                Set<String> outPaths = getPaths(outPathMap, outLength, expectedLength, 1);
                if (!outPaths.isEmpty() && !inPaths.isEmpty()) {
                    for (String outPath : outPaths) {
                        for (String inPath : inPaths) {
                            String pathStr = inPath + outPath;
                            if (!skipOutput) {
                                context.take(ObjectRow.create(pathStr));
                            }
                        }
                    }
                }
            }
        }
    }

    public static Set<String> getPaths(Map<Integer, List<Object[]>> pathMap, int expectLength, int maxLength, int idr) {
        Set<String> paths = new HashSet<>();
        for (int length = expectLength; length <= maxLength; length++) {
            List<Object[]> currentPaths = pathMap.get(length);
            if (currentPaths == null) {
                continue;
            }
            for (Object[] path : currentPaths) {
                StringBuilder sb = new StringBuilder();
                if (idr == 0) {
                    int offset = path.length - expectLength;
                    for (int i = 0; i < expectLength - 1; i++) {
                        sb.append(path[offset + i]).append(",");
                    }
                    paths.add(sb.toString());
                } else {
                    int arrayLength = path.length;
                    for (int i = 0; i < expectLength; i++) {
                        sb.append(path[arrayLength - i - 1]).append(",");
                    }
                    paths.add(sb.toString());
                }
            }
        }
        return paths;
    }

    public static String convertToPath(Object[] outPath, int outLength, int[] inPath, int inLength) {
        StringBuilder sb = new StringBuilder();
        if (inLength > 1) {
            int offset = inPath.length - inLength;
            for (int i = 0; i < inLength - 1; i++) {
                sb.append(inPath[offset + i]).append(",");
            }
        }
        int arrayLength = outPath.length;
        for (int i = 0; i < outLength; i++) {
            sb.append(outPath[arrayLength - i - 1]).append(",");
        }
        return sb.toString();
    }
}
