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

package org.apache.geaflow.core.graph;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import org.apache.geaflow.partitioner.IPartitioner;
import org.apache.geaflow.plan.graph.AffinityLevel;
import org.apache.geaflow.plan.graph.VertexType;
import org.apache.geaflow.processor.Processor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ExecutionVertex implements Serializable {

    private static final Logger LOGGER = LoggerFactory.getLogger(ExecutionVertex.class);

    private int vertexId;
    private Processor processor;
    private String name;
    private int parallelism;
    private int maxParallelism;
    private List<Integer> parentVertexIds;
    private List<Integer> childrenVertexIds;
    private int numPartitions;
    private VertexType vertexType;
    private AffinityLevel affinityLevel;
    private VertexType chainTailType;

    private List<ExecutionEdge> inputEdges;
    private List<ExecutionEdge> outputEdges;

    public ExecutionVertex(int vertexId, String name) {
        this.vertexId = vertexId;
        this.name = name;
        this.parentVertexIds = new ArrayList<>();
        this.childrenVertexIds = new ArrayList<>();
        this.inputEdges = null;
        this.outputEdges = null;
        this.affinityLevel = AffinityLevel.worker;
    }

    public int getVertexId() {
        return vertexId;
    }

    public void setVertexId(int vertexId) {
        this.vertexId = vertexId;
    }

    public Processor getProcessor() {
        return processor;
    }

    public void setProcessor(Processor processor) {
        this.processor = processor;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getParallelism() {
        return parallelism;
    }

    public void setParallelism(int parallelism) {
        this.parallelism = parallelism;
    }

    public int getMaxParallelism() {
        return maxParallelism;
    }

    public void setMaxParallelism(int maxParallelism) {
        this.maxParallelism = maxParallelism;
    }

    public List<Integer> getParentVertexIds() {
        return parentVertexIds;
    }

    public void setParentVertexIds(List<Integer> parentVertexIds) {
        this.parentVertexIds = parentVertexIds;
    }

    public List<Integer> getChildrenVertexIds() {
        return childrenVertexIds;
    }

    public void setChildrenVertexIds(List<Integer> childrenVertexIds) {
        this.childrenVertexIds = childrenVertexIds;
    }

    public int getNumPartitions() {
        return numPartitions;
    }

    public void setNumPartitions(int numPartitions) {
        this.numPartitions = numPartitions;
    }

    public List<ExecutionEdge> getInputEdges() {
        return inputEdges;
    }

    public void setInputEdges(List<ExecutionEdge> inputEdges) {
        this.inputEdges = inputEdges;
    }

    public List<ExecutionEdge> getOutputEdges() {
        return outputEdges;
    }

    public void setOutputEdges(List<ExecutionEdge> outputEdges) {
        this.outputEdges = outputEdges;
    }

    public VertexType getVertexType() {
        return vertexType;
    }

    public void setVertexType(VertexType vertexType) {
        this.vertexType = vertexType;
    }

    public AffinityLevel getAffinityLevel() {
        return affinityLevel;
    }

    public void setAffinityLevel(AffinityLevel affinityLevel) {
        this.affinityLevel = affinityLevel;
    }

    public VertexType getChainTailType() {
        return chainTailType;
    }

    public void setChainTailType(VertexType chainTailType) {
        this.chainTailType = chainTailType;
    }

    public boolean isRepartition() {
        if (outputEdges != null) {
            boolean isAllForward = this.outputEdges.stream().allMatch(
                x -> x.getPartitioner().getPartitionType() == IPartitioner.PartitionType.forward);
            if (isAllForward) {
                return false;
            } else {
                return true;
            }
        }
        return true;
    }

    @Override
    public String toString() {
        return "ExecutionVertex{" + "vertexId=" + vertexId + ", processor=" + processor + ", name='"
            + name + '\'' + '}';
    }
}
