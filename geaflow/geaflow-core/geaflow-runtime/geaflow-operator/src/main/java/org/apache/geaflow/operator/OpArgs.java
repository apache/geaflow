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

package org.apache.geaflow.operator;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class OpArgs implements Serializable {

    private int opId;
    private String opName;
    private int parallelism;
    private boolean windowOp;
    private Map config;
    private OpType opType;
    private ChainStrategy chainStrategy;
    // Specify that the operator whether can group together.
    private boolean enGroup = true;

    public OpArgs() {
        this.config = new HashMap();
    }

    public int getOpId() {
        return opId;
    }

    public void setOpId(int opId) {
        this.opId = opId;
    }

    public String getOpName() {
        return opName;
    }

    public void setOpName(String opName) {
        this.opName = opName;
    }

    public int getParallelism() {
        return parallelism;
    }

    public void setParallelism(int parallelism) {
        this.parallelism = parallelism;
    }

    public Map getConfig() {
        return config;
    }

    public void setConfig(Map config) {
        this.config = config;
    }

    public void addConfig(String key, String value) {
        if (this.config == null) {
            this.config = new HashMap();
        }
        this.config.put(key, value);
    }

    public void setOpType(OpType type) {
        this.opType = type;
    }

    public OpType getOpType() {
        return opType;
    }

    public void setChainStrategy(ChainStrategy chainStrategy) {
        this.chainStrategy = chainStrategy;
    }

    public ChainStrategy getChainStrategy() {
        return chainStrategy;
    }

    public boolean isEnGroup() {
        return enGroup;
    }

    public void setEnGroup(boolean enGroup) {
        this.enGroup = enGroup;
    }

    public enum ChainStrategy {

        /**
         * This policy indicates that an operator cannot have a leading operator
         * and can be used as a chain header for other operators.
         */
        HEAD,
        /**
         * This policy does not allow an operator to be linked to a leading operator
         * or to be used as a leading operator of another operator.
         * This means that the chain can only have one operator.
         */
        NEVER,
        /**
         * Always.
         */
        ALWAYS,
    }

    public enum OpType {
        /**
         * Single window source that indicates all window.
         */
        SINGLE_WINDOW_SOURCE,
        /**
         * Multi window source.
         */
        MULTI_WINDOW_SOURCE,
        /**
         * Graph source.
         */
        GRAPH_SOURCE,
        /**
         * Re partition.
         */
        RE_PARTITION,
        /**
         * One input.
         */
        ONE_INPUT,
        /**
         * Two input.
         */
        TWO_INPUT,
        /**
         * Vertex centric compute.
         */
        VERTEX_CENTRIC_COMPUTE,
        /**
         * Vertex centric traversal.
         */
        VERTEX_CENTRIC_TRAVERSAL,
        /**
         * Incremental vertex centric compute.
         */
        INC_VERTEX_CENTRIC_COMPUTE,
        /**
         * Incremental vertex centric traversal.
         */
        INC_VERTEX_CENTRIC_TRAVERSAL,
        /**
         * Vertex centric compute with agg.
         */
        VERTEX_CENTRIC_COMPUTE_WITH_AGG,
    }

}
