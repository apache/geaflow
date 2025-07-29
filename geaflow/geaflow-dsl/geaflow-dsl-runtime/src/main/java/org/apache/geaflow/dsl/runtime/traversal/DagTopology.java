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

package org.apache.geaflow.dsl.runtime.traversal;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.geaflow.dsl.common.data.StepRecord;
import org.apache.geaflow.dsl.runtime.traversal.operator.StepOperator;

public class DagTopology implements Serializable {

    /**
     * The name of query that this DAG represents.
     */
    private final String queryName;

    private final StepOperator<StepRecord, StepRecord> entryOperator;

    /**
     * The output node ids for each node in the DAG.
     */
    private final Map<Long, List<Long>> opId2OutputIds;

    /**
     * The input node ids for each node in the DAG.
     */
    private final Map<Long, List<Long>> opId2InputIds;

    private final Set<Pair<Long, Long>> chainableOpIdPairs;

    private final Map<Long, StepOperator<StepRecord, StepRecord>> opId2Operators;

    private DagTopology(String queryName,
                        StepOperator<StepRecord, StepRecord> entryOperator,
                        Set<Pair<Long, Long>> chainableOpIdPairs,
                        Map<Long, List<Long>> opId2InputIds,
                        Map<Long, List<Long>> opId2OutputIds,
                        Map<Long, StepOperator<StepRecord, StepRecord>> opId2Operators) {
        this.queryName = queryName;
        this.entryOperator = entryOperator;
        this.chainableOpIdPairs = chainableOpIdPairs;
        this.opId2InputIds = opId2InputIds;
        this.opId2OutputIds = opId2OutputIds;
        this.opId2Operators = opId2Operators;
    }

    @SuppressWarnings("unchecked")
    public static DagTopology build(String queryName, StepLogicalPlan logicalPlan) {
        Set<Pair<Long, Long>> chainableOpIdPairs = new HashSet<>();
        Map<Long, List<Long>> opId2InputIds = new HashMap<>();
        Map<Long, List<Long>> opId2OutputIds = new HashMap<>();
        Map<Long, StepOperator<StepRecord, StepRecord>> opId2Operators = new HashMap<>();

        generateChainOpIds(logicalPlan, chainableOpIdPairs);
        generateLogicalDependency(logicalPlan, opId2InputIds, opId2OutputIds);
        addNextOperators(logicalPlan, opId2Operators);
        return new DagTopology(queryName, logicalPlan.getHeadPlan().getOperator(), chainableOpIdPairs,
            opId2InputIds, opId2OutputIds, opId2Operators);
    }

    private static void generateLogicalDependency(StepLogicalPlan endPlan,
                                                  Map<Long, List<Long>> opId2InputIds,
                                                  Map<Long, List<Long>> opId2OutputIds) {
        for (StepLogicalPlan inputPlan : endPlan.getInputs()) {
            List<Long> outputIds = opId2OutputIds.computeIfAbsent(inputPlan.getId(),
                k -> new ArrayList<>());
            if (!outputIds.contains(endPlan.getId())) {
                outputIds.add(endPlan.getId());
                opId2OutputIds.put(inputPlan.getId(), outputIds);
            }
            List<Long> inputIds = opId2InputIds.computeIfAbsent(endPlan.getId(),
                k -> new ArrayList<>());
            if (!inputIds.contains(inputPlan.getId())) {
                inputIds.add(inputPlan.getId());
                opId2InputIds.put(endPlan.getId(), inputIds);
            }

            generateLogicalDependency(inputPlan, opId2InputIds, opId2OutputIds);
        }
    }

    private static void generateChainOpIds(StepLogicalPlan endPlan, Set<Pair<Long, Long>> chainableOpIdPairs) {
        for (StepLogicalPlan inputPlan : endPlan.getInputs()) {
            generateChainOpIds(inputPlan, chainableOpIdPairs);
        }

        if (endPlan.getInputs().isEmpty()) {
            chainableOpIdPairs.add(Pair.of(endPlan.getId(), endPlan.getId()));
        } else {
            for (StepLogicalPlan inputPlan : endPlan.getInputs()) {
                if (inputPlan.isAllowChain()) {
                    chainableOpIdPairs.add(Pair.of(endPlan.getId(), inputPlan.getId()));
                }
            }
        }
    }

    @SuppressWarnings("unchecked")
    private static void addNextOperators(StepLogicalPlan endPlan,
                                         Map<Long, StepOperator<StepRecord, StepRecord>> opId2Operators) {
        opId2Operators.put(endPlan.getId(), endPlan.getOperator());
        for (StepLogicalPlan inputPlan : endPlan.getInputs()) {
            inputPlan.getOperator().addNextOperator(endPlan.getOperator());
            addNextOperators(inputPlan, opId2Operators);
        }
    }

    public String getQueryName() {
        return queryName;
    }

    public StepOperator<StepRecord, StepRecord> getEntryOperator() {
        return entryOperator;
    }

    public boolean contains(long opId) {
        return opId2Operators.containsKey(opId);
    }

    public List<Long> getOutputIds(long opId) {
        return opId2OutputIds.getOrDefault(opId, new ArrayList<>());
    }

    public List<Long> getInputIds(long opId) {
        return opId2InputIds.getOrDefault(opId, new ArrayList<>());
    }

    public boolean isChained(long opId1, long opId2) {
        return chainableOpIdPairs.contains(Pair.of(opId1, opId2))
            || chainableOpIdPairs.contains(Pair.of(opId2, opId1));
    }

    public Map<Long, StepOperator<StepRecord, StepRecord>> getOpId2Operators() {
        return opId2Operators;
    }

    @SuppressWarnings("unchecked")
    public <IN extends StepRecord, OUT extends StepRecord> StepOperator<IN, OUT> getOperator(long opId) {
        return (StepOperator<IN, OUT>) opId2Operators.get(opId);
    }

    public long getEntryOpId() {
        return entryOperator.getId();
    }
}
