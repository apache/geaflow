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

package org.apache.geaflow.plan.graph;

import java.io.Serializable;
import java.util.Objects;
import org.apache.geaflow.operator.Operator;
import org.apache.geaflow.operator.base.AbstractOperator;

public class PipelineVertex<OP extends Operator> implements Serializable {

    private int vertexId;
    private OP operator;
    private int parallelism;
    private long iterations;
    private VertexType type;
    private VertexMode vertexMode;
    private AffinityLevel affinity;
    private boolean duplication;
    private VertexType chainTailType;
    /**
     * Co-location group ID for local shuffle optimization.
     * Vertices with the same co-location group ID should be deployed on the same node
     * to enable automatic local shuffle through LocalInputChannel.
     */
    private String coLocationGroup;

    public PipelineVertex(int vertexId, OP operator, int parallelism) {
        this.vertexId = vertexId;
        this.operator = operator;
        this.parallelism = parallelism;
        this.iterations = 1;
        this.affinity = AffinityLevel.worker;
    }

    public PipelineVertex(int vertexId, OP operator, VertexType type, int parallelism) {
        this(vertexId, operator, parallelism);
        this.type = type;
        this.chainTailType = type;
    }

    public PipelineVertex(int vertexId, VertexType vertexType, OP operator, VertexMode vertexMode) {
        this(vertexId, operator, vertexType, ((AbstractOperator) operator).getOpArgs().getParallelism());
        this.vertexMode = vertexMode;
    }

    public boolean isDuplication() {
        return duplication;
    }

    public void setDuplication() {
        this.duplication = true;
    }

    public int getParallelism() {
        return parallelism;
    }

    public void setParallelism(int parallelism) {
        this.parallelism = parallelism;
    }

    public int getVertexId() {
        return vertexId;
    }

    public void setVertexId(int vertexId) {
        this.vertexId = vertexId;
    }

    public OP getOperator() {
        return operator;
    }

    public void setOperator(OP operator) {
        this.operator = operator;
    }

    public VertexType getType() {
        return type;
    }

    public void setType(VertexType type) {
        this.type = type;
        this.chainTailType = type;
    }

    public String getName() {
        if (this.operator != null) {
            return this.operator.getClass().getSimpleName();
        }
        return null;
    }

    public VertexMode getVertexMode() {
        return vertexMode;
    }

    public void setVertexMode(VertexMode vertexMode) {
        this.vertexMode = vertexMode;
    }

    public String getVertexName() {
        return "node-" + this.vertexId;
    }

    public long getIterations() {
        return iterations;
    }

    public void setIterations(long iterations) {
        this.iterations = iterations;
    }

    public AffinityLevel getAffinity() {
        return affinity;
    }

    public void setAffinity(AffinityLevel affinity) {
        this.affinity = affinity;
    }

    public VertexType getChainTailType() {
        return chainTailType;
    }

    public void setChainTailType(VertexType chainTailType) {
        this.chainTailType = chainTailType;
    }

    public String getCoLocationGroup() {
        return coLocationGroup;
    }

    public void setCoLocationGroup(String coLocationGroup) {
        this.coLocationGroup = coLocationGroup;
    }

    public String getVertexString() {
        String operatorStr = operator.toString();
        return String.format("%s, p:%d, %s", getVertexName(), parallelism, operatorStr);
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.getVertexId());
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof PipelineVertex) {
            PipelineVertex other = (PipelineVertex) obj;
            if (other.getVertexId() == this.vertexId) {
                return true;
            }
        }
        return false;
    }
}
