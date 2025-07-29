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

package org.apache.geaflow.plan.optimizer;

import com.google.common.base.Preconditions;
import com.google.common.collect.ArrayListMultimap;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.commons.io.input.ClassLoaderObjectInputStream;
import org.apache.commons.lang3.StringUtils;
import org.apache.geaflow.common.errorcode.RuntimeErrors;
import org.apache.geaflow.common.exception.GeaflowRuntimeException;
import org.apache.geaflow.common.serialize.SerializerFactory;
import org.apache.geaflow.operator.base.AbstractOperator;
import org.apache.geaflow.operator.impl.window.FilterOperator;
import org.apache.geaflow.operator.impl.window.KeySelectorOperator;
import org.apache.geaflow.operator.impl.window.MapOperator;
import org.apache.geaflow.operator.impl.window.SinkOperator;
import org.apache.geaflow.operator.impl.window.UnionOperator;
import org.apache.geaflow.partitioner.IPartitioner;
import org.apache.geaflow.plan.graph.PipelineEdge;
import org.apache.geaflow.plan.graph.PipelineGraph;
import org.apache.geaflow.plan.graph.PipelineVertex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UnionOptimizer implements Serializable {

    private static final Logger LOGGER = LoggerFactory.getLogger(UnionOptimizer.class);

    private Map<Integer, PipelineVertex> vertexMap;
    private Map<Integer, Set<PipelineEdge>> outputEdges;
    private Map<Integer, Set<PipelineEdge>> inputEdges;

    private Set<PipelineVertex> visited = new HashSet<>();

    private Set<PipelineVertex> newVertices = new HashSet<>();
    private Set<PipelineEdge> newEdges = new LinkedHashSet<>();
    private Map<Integer, PipelineVertex> newVertexMap = new HashMap<>();

    private boolean needOptimize;

    private List statelessOperator = new ArrayList<>(
        Arrays.asList(FilterOperator.class, MapOperator.class, UnionOperator.class,
            KeySelectorOperator.class));

    private PipelineVertex unionVertex;
    private int removeUnionNum = 0;

    public UnionOptimizer(boolean extraOptimizeSink) {
        LOGGER.info("extraOptimizeSink {}", extraOptimizeSink);
        if (extraOptimizeSink) {
            statelessOperator.add(SinkOperator.class);
        }
    }

    private void init(PipelineGraph plan) {
        this.vertexMap = plan.getVertexMap();
        this.outputEdges = plan.getVertexOutputEdges();
        this.inputEdges = plan.getVertexInputEdges();
        this.needOptimize = false;
    }

    /**
     * Union push-up algorithm: 1. DFS push up vertex, 2. Kahn split node, 3. Rearrange.
     * All operators are rearranged to ensure algorithm reentrant.
     */
    public boolean optimizePlan(PipelineGraph plan) {
        init(plan);

        if (!pushUpPartitionFunction(plan)) {
            return false;
        }
        try {
            plan.getSourceVertices().forEach(this::dfs);
            if (!needOptimize) {
                return false;
            }
            kahn(plan.getSourceVertices(), 0);
        } catch (Exception ex) {
            LOGGER.warn("Unexpected exception happened while optimizing, thus give up", ex);
            return false;
        }

        /**
         * Check Validation.
         * We use dfs to find union vertex lacking global view, which can lead to wrong results.
         */
        if (newVertices.size() + removeUnionNum < plan.getVertexMap().size()) {
            LOGGER.warn(String.format(
                    "vertices number %s plus remove union vertices num %s is smaller after optimization %s, "
                        + "this is not right, thus give up"),
                newVertices.size(), removeUnionNum,
                plan.getVertexMap().size());
            return false;
        }

        plan.setPipelineEdges(newEdges);
        plan.setPipelineVertices(newVertices);
        return true;
    }

    private boolean pushUpPartitionFunction(PipelineGraph plan) {
        for (PipelineVertex vertex : plan.getPipelineVertices()) {
            if (vertex.getOperator() instanceof UnionOperator) {
                PipelineEdge edge = getOutEdgeFromVertex(vertex);
                if (edge != null && !edge.getPartition().getPartitionType().isEnablePushUp()) {
                    IPartitioner partitionFunction = edge.getPartition();
                    edge.setPartitionType(IPartitioner.PartitionType.forward);
                    for (PipelineEdge inEdge : plan.getVertexInputEdges().get(vertex.getVertexId())) {
                        if (inEdge.getPartitionType() == IPartitioner.PartitionType.key
                            || inEdge.getPartition() != null) {
                            return false;
                        }
                        inEdge.setPartition(partitionFunction);
                        edge.setPartitionType(IPartitioner.PartitionType.key);
                    }
                }
            }
        }
        return true;
    }

    /**
     * Push up vertex, and try to remove union vertex.
     */
    private void dfs(PipelineVertex vertex) {
        if (!visited.add(vertex)) {
            return;
        }
        LOGGER.debug("visit vertex {}", vertex);
        if (needPushUp(vertex)) {
            pushUp(vertex);
            vertex = unionVertex;
            this.needOptimize = true;
        } else if (unionVertex != null) {
            tryRemoveVertex(unionVertex);
            unionVertex = null;
        }

        for (PipelineEdge executeEdge : outputEdges.get(vertex.getVertexId())) {
            PipelineVertex nextVertex = vertexMap.get(executeEdge.getTargetId());
            dfs(nextVertex);
        }
    }

    private PipelineEdge getOutEdgeFromVertex(PipelineVertex vertex) {
        Iterator<PipelineEdge> it = outputEdges.get(vertex.getVertexId()).iterator();
        if (it.hasNext()) {
            return it.next();
        }
        return null;
    }

    /**
     * Operator push up.
     * The outgoing edge has been modified at setTargetId, so only need to focus on the incoming edge.
     */
    private void pushUp(PipelineVertex sVertex) {
        sVertex.setDuplication();
        if (sVertex.equals(unionVertex)) {
            return;
        }
        LOGGER.info("pushUp vertex {}", sVertex);

        PipelineEdge unionOutEdge = getOutEdgeFromVertex(unionVertex);
        Preconditions.checkNotNull(unionOutEdge);
        // SOutEdge can be null.
        PipelineEdge sOutEdge = getOutEdgeFromVertex(sVertex);

        // Incoming edge of sVertex modify.
        inputEdges.get(sVertex.getVertexId()).remove(unionOutEdge);
        for (PipelineEdge unionInEdge : inputEdges.get(unionVertex.getVertexId())) {
            unionInEdge.setTargetId(sVertex.getVertexId());
            inputEdges.get(sVertex.getVertexId()).add(unionInEdge);
        }

        // Original input edge of sOutEdge modify.
        if (sOutEdge != null) {
            inputEdges.get(sOutEdge.getTargetId()).remove(sOutEdge);
            unionOutEdge.setTargetId(sOutEdge.getTargetId());
            inputEdges.get(sOutEdge.getTargetId()).add(unionOutEdge);
        }

        // The incoming edge of unionVertex modify.
        inputEdges.get(unionVertex.getVertexId()).clear();
        if (sOutEdge != null) {
            sOutEdge.setTargetId(unionVertex.getVertexId());
            inputEdges.get(unionVertex.getVertexId()).add(sOutEdge);
        }

        // Key partition pass.
        if (sOutEdge != null && sOutEdge.getPartitionType().equals(IPartitioner.PartitionType.key)) {
            unionOutEdge.setStreamOrdinal(sOutEdge.getStreamOrdinal());
            unionOutEdge.setPartitionType(IPartitioner.PartitionType.key);
            sOutEdge.setPartitionType(IPartitioner.PartitionType.forward);
        }

        tryRemoveVertex(sVertex);
    }

    private boolean needPushUp(PipelineVertex vertex) {

        // 1. Multi output edges not process.
        if (outputEdges.get(vertex.getVertexId()).size() > 1) {
            return false;
        }

        // If partition type is custom then not push up.
        for (PipelineEdge edge : outputEdges.get(vertex.getVertexId())) {
            if (edge.getPartition() != null && !edge.getPartition().getPartitionType().isEnablePushUp()) {
                return false;
            }
        }

        // 2. Upstream has union vertex and downstream vertex exists.
        if (unionVertex != null && getOutVertexIdSet(unionVertex).contains(vertex.getVertexId())
            && statelessOperator.contains(vertex.getOperator().getClass())) {
            return true;
        }
        if (vertex.getOperator() instanceof UnionOperator) {
            unionVertex = vertex;
            return true;
        }
        return false;
    }

    /**
     * Try remove union vertex, modify incoming edge relation of downstream, upstream modify targetId of edge directly.
     * If outgoing edge of union vertex carries key property, we need pass to upstream.
     */
    private void tryRemoveVertex(PipelineVertex vertex) {
        LOGGER.debug("try remove vertex {}", vertex);
        if (vertex.isDuplication() && vertex.getOperator() instanceof UnionOperator
            && outputEdges.get(vertex.getVertexId()).size() == 1) {
            LOGGER.info("remove vertex {}", vertex);
            removeUnionNum++;
            PipelineEdge outputEdge = outputEdges.get(vertex.getVertexId()).iterator().next();
            int targetId = outputEdge.getTargetId();
            inputEdges.get(targetId).remove(outputEdge);

            for (PipelineEdge inEdge : inputEdges.get(vertex.getVertexId())) {
                inEdge.setTargetId(targetId);
                inputEdges.get(targetId).add(inEdge);
                if (outputEdge.getPartitionType() == IPartitioner.PartitionType.key) {
                    inEdge.setPartitionType(IPartitioner.PartitionType.key);
                }
            }
        }
    }

    /**
     * Topological sorting is used to ensure the sequence of ids.
     */
    private void kahn(List<PipelineVertex> vertices, int id) throws IOException, ClassNotFoundException {
        Queue<PipelineVertex> toVisitQueue = new ArrayDeque<>(vertices);
        Set<PipelineEdge> visitedEdge = new HashSet<>();
        ArrayListMultimap<Integer, Integer> oldIdToNewIdMap = ArrayListMultimap.create();

        while (!toVisitQueue.isEmpty()) {
            PipelineVertex vertex = toVisitQueue.poll();
            if (!vertex.isDuplication()) {
                id++;
                PipelineVertex newVertex = cloneVertex(vertex, id, 0);
                newVertices.add(newVertex);
                newVertexMap.put(id, newVertex);
                oldIdToNewIdMap.put(vertex.getVertexId(), id);
            }

            // Consider the condition that in edge and the previous node fission.
            int index = 0;
            for (PipelineEdge inEdge : inputEdges.get(vertex.getVertexId())) {
                PipelineVertex oriSrcVertex = vertexMap.get(inEdge.getSrcId());
                int oriSrcId = oriSrcVertex.getVertexId();
                // The previous node fission.
                for (Integer srcId : oldIdToNewIdMap.get(oriSrcId)) {
                    if (vertex.isDuplication()) {
                        id++;
                        // The parallelism of the new upstream node is used to ensure vertex merging.
                        PipelineVertex newVertex = cloneVertex(vertex, id,
                            newVertexMap.get(srcId).getParallelism());
                        // After the vertex is split, we need to change its name,
                        // otherwise the concurrency cannot be set. Sink has side effects, do not do treatment.
                        if (!(newVertex.getOperator() instanceof SinkOperator)) {
                            changeOperatorName(newVertex, index++);
                        }
                        newVertices.add(newVertex);
                        newVertexMap.put(id, newVertex);
                        oldIdToNewIdMap.put(vertex.getVertexId(), id);
                    }
                    PipelineEdge newEdge = new PipelineEdge(srcId, srcId, id, inEdge.getPartition(),
                        inEdge.getStreamOrdinal(), inEdge.getEncoder());
                    if (vertexMap.get(oriSrcId).isDuplication()) {
                        IPartitioner partitionFunction = newEdge.getPartition();
                        newEdge.setEdgeName(String
                            .format("union-%d-%s-%s-%s", id, newEdge.getPartitionType(),
                                PipelineEdge.JoinStream.values()[newEdge.getStreamOrdinal()],
                                partitionFunction != null ? partitionFunction.getClass()
                                    .getSimpleName() : "none"));
                    }
                    newEdges.add(newEdge);
                }
            }

            for (PipelineEdge executeEdge : outputEdges.get(vertex.getVertexId())) {
                PipelineVertex nextVertex = vertexMap.get(executeEdge.getTargetId());
                visitedEdge.add(executeEdge);
                if (visitedEdge.containsAll(inputEdges.get(nextVertex.getVertexId()))) {
                    toVisitQueue.add(nextVertex);
                }
            }
        }
    }

    private byte[] toByteArray(Object obj) throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(bos);
        oos.writeObject(obj);
        oos.flush();
        byte[] bytes = bos.toByteArray();
        oos.close();
        bos.close();
        return bytes;
    }

    private Object toObject(byte[] bytes) throws IOException, ClassNotFoundException {
        ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
        bis.mark(0);
        ObjectInputStream ois = new ObjectInputStream(bis);
        Object obj = null;
        try {
            obj = ois.readObject();
        } catch (Exception ex) {
            bis.reset();
            ois = new ClassLoaderObjectInputStream(Thread.currentThread().getContextClassLoader(),
                bis);
            obj = ois.readObject();
        } finally {
            ois.close();
        }
        bis.close();
        return obj;
    }

    private PipelineVertex cloneVertex(PipelineVertex vertex, int id, int parallelism)
        throws IOException, ClassNotFoundException {
        LOGGER.debug("clone Vertex {}", vertex);
        PipelineVertex cloned;
        try {
            byte[] out = SerializerFactory.getKryoSerializer().serialize(vertex);
            cloned = (PipelineVertex) SerializerFactory.getKryoSerializer().deserialize(out);
        } catch (Exception ex) {
            LOGGER.warn("vertex {} kryo fail, try java serde, ex: {}", vertex,
                Arrays.toString(ex.getStackTrace()));
            cloned = (PipelineVertex) toObject(toByteArray(vertex));
            if (cloned == null) {
                throw new GeaflowRuntimeException(
                    RuntimeErrors.INST.undefinedError(vertex.getVertexString() + " is not Serializable"), ex);
            }
        }
        cloned.setVertexId(id);
        AbstractOperator abstractOperator = ((AbstractOperator) cloned.getOperator());
        abstractOperator.getOpArgs().setOpId(id);
        abstractOperator.setFunction(((AbstractOperator) vertex.getOperator()).getFunction());

        if (parallelism != 0) {
            cloned.setParallelism(parallelism);
            abstractOperator.getOpArgs().setParallelism(parallelism);
        }

        return cloned;
    }

    private void changeOperatorName(PipelineVertex vertex, int index) {
        if (StringUtils.isNotEmpty(((AbstractOperator) vertex.getOperator()).getOpArgs().getOpName())) {
            ((AbstractOperator) vertex.getOperator()).getOpArgs()
                .setOpName(String.format("%s-%d", ((AbstractOperator) vertex.getOperator()).getOpArgs().getOpName(), index));
        }
    }

    private Set<Integer> getOutVertexIdSet(PipelineVertex vertex) {
        return outputEdges.get(vertex.getVertexId()).stream().map(PipelineEdge::getTargetId)
            .collect(Collectors.toSet());
    }
}
