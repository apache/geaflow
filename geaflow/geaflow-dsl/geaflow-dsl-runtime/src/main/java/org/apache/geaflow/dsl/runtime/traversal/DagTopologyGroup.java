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

import com.google.common.collect.Lists;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.geaflow.dsl.common.data.StepRecord;
import org.apache.geaflow.dsl.common.exception.GeaFlowDSLException;
import org.apache.geaflow.dsl.runtime.traversal.operator.StepLoopUntilOperator;
import org.apache.geaflow.dsl.runtime.traversal.operator.StepOperator;

public class DagTopologyGroup {

    private final DagTopology mainDag;

    private final Map<String, DagTopology> subDags;

    private final Map<Long, StepOperator<StepRecord, StepRecord>> globalOpId2Operators;

    public DagTopologyGroup(DagTopology mainDag,
                            Map<String, DagTopology> subDags) {
        this.mainDag = mainDag;
        this.subDags = subDags;
        this.globalOpId2Operators = new HashMap<>();
        this.globalOpId2Operators.putAll(mainDag.getOpId2Operators());

        for (DagTopology subDag : subDags.values()) {
            Map<Long, StepOperator<StepRecord, StepRecord>> id2Operators = subDag.getOpId2Operators();
            for (Map.Entry<Long, StepOperator<StepRecord, StepRecord>> entry : id2Operators.entrySet()) {
                long opId = entry.getKey();
                if (globalOpId2Operators.containsKey(opId)) {
                    throw new GeaFlowDSLException("Operator id: " + opId + " in sub dag: " + subDag.getQueryName()
                        + " is conflict with other dag.");
                }
                globalOpId2Operators.put(opId, entry.getValue());
            }
        }
    }

    public List<Long> getOutputIds(long opId) {
        if (mainDag.contains(opId)) {
            return mainDag.getOutputIds(opId);
        }
        for (DagTopology subDag : subDags.values()) {
            if (subDag.contains(opId)) {
                return subDag.getOutputIds(opId);
            }
        }
        throw new IllegalArgumentException("Illegal opId: " + opId);
    }

    public List<Long> getInputIds(long opId) {
        if (mainDag.contains(opId)) {
            return mainDag.getInputIds(opId);
        }
        for (DagTopology subDag : subDags.values()) {
            if (subDag.contains(opId)) {
                return subDag.getInputIds(opId);
            }
        }
        throw new IllegalArgumentException("Illegal opId: " + opId);
    }

    public DagTopology getDagTopology(long opId) {
        if (mainDag.contains(opId)) {
            return mainDag;
        }
        for (DagTopology subDag : subDags.values()) {
            if (subDag.contains(opId)) {
                return subDag;
            }
        }
        throw new IllegalArgumentException("Illegal opId: " + opId);
    }

    public boolean isChained(long opId1, long opId2) {
        if (mainDag.contains(opId1) && mainDag.contains(opId2)) {
            return mainDag.isChained(opId1, opId2);
        }
        for (DagTopology subDag : subDags.values()) {
            if (subDag.contains(opId1) && subDag.contains(opId2)) {
                return subDag.isChained(opId1, opId2);
            }
        }
        return false;
    }

    public boolean belongMainDag(long opId) {
        return mainDag.contains(opId);
    }

    @SuppressWarnings("unchecked")
    public StepOperator getOperator(long opId) {
        return globalOpId2Operators.get(opId);
    }

    public DagTopology getMainDag() {
        return mainDag;
    }

    public List<DagTopology> getAllDagTopology() {
        List<DagTopology> dagTopologies = new ArrayList<>();
        dagTopologies.add(mainDag);
        dagTopologies.addAll(subDags.values());
        return dagTopologies;
    }

    public List<DagTopology> getSubDagTopologies() {
        return Lists.newArrayList(subDags.values());
    }

    public Collection<StepOperator<StepRecord, StepRecord>> getAllOperators() {
        return globalOpId2Operators.values();
    }

    public int getIterationCount(int currentDepth, StepOperator stepOperator) {
        List<String> subQueryNames = stepOperator.getSubQueryNames();
        int maxSubDagIteration = 0;
        for (String subQueryName : subQueryNames) {
            DagTopology subDag = this.subDags.get(subQueryName);
            assert subDag != null;
            int subDagIterationCount =
                addIteration(getIterationCount(1, subDag.getEntryOperator()), 1);
            if (subDagIterationCount > maxSubDagIteration) {
                maxSubDagIteration = subDagIterationCount;
            }
        }
        currentDepth = addIteration(currentDepth, maxSubDagIteration);

        if (stepOperator instanceof StepLoopUntilOperator) {
            StepLoopUntilOperator loopUntilOperator = (StepLoopUntilOperator) stepOperator;
            currentDepth = addIteration(currentDepth,
                addIteration(loopUntilOperator.getMaxLoopCount(), 1));
        }
        int depth = currentDepth;
        for (Object op : stepOperator.getNextOperators()) {
            StepOperator next = (StepOperator) op;
            int branchDepth = getIterationCount(currentDepth, next);
            if (!isChained(stepOperator.getId(), next.getId())) {
                branchDepth = addIteration(branchDepth, 1);
            }
            if (branchDepth > depth) {
                depth = branchDepth;
            }
        }
        return depth;
    }

    private static int addIteration(int iteration, int delta) {
        if (iteration == Integer.MAX_VALUE || iteration < 0 || delta == 0) {
            return iteration;
        }
        if (delta > 0) {
            if (Integer.MAX_VALUE - iteration >= delta) {
                return iteration + delta;
            } else {
                return Integer.MAX_VALUE;
            }
        } else {
            if (iteration + delta >= 0) {
                return iteration + delta;
            } else {
                return iteration;
            }
        }
    }
}
