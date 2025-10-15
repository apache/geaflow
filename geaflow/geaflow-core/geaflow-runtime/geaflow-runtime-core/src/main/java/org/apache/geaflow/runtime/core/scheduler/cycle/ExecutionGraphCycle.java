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

package org.apache.geaflow.runtime.core.scheduler.cycle;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.common.exception.GeaflowRuntimeException;
import org.apache.geaflow.ha.runtime.HighAvailableLevel;

public class ExecutionGraphCycle extends AbstractExecutionCycle {

    private final String driverId;
    private final int driverIndex;
    private final Map<Integer, IExecutionCycle> cycleMap;
    private final Map<Integer, List<Integer>> cycleChildren;
    private final Map<Integer, List<Integer>> cycleParents;
    private HighAvailableLevel haLevel;

    public ExecutionGraphCycle(long schedulerId, long pipelineId, long pipelineTaskId,
                               String pipelineName, int cycleId, int flyingCount, long iterationCount,
                               Configuration config, String driverId, int driverIndex) {
        super(schedulerId, pipelineId, pipelineTaskId, pipelineName, cycleId, flyingCount, iterationCount, config);
        this.driverId = driverId;
        this.driverIndex = driverIndex;
        this.cycleMap = new HashMap<>();
        this.cycleChildren = new HashMap<>();
        this.cycleParents = new HashMap<>();
        this.haLevel = HighAvailableLevel.REDO;
    }

    @Override
    public ExecutionCycleType getType() {
        return ExecutionCycleType.GRAPH;
    }

    @Override
    public String getDriverId() {
        return driverId;
    }

    @Override
    public int getDriverIndex() {
        return driverIndex;
    }

    @Override
    public HighAvailableLevel getHighAvailableLevel() {
        return haLevel;
    }

    public void addCycle(IExecutionCycle cycle, boolean skipCheckpoint) {
        if (cycleMap.containsKey(cycle.getCycleId())) {
            throw new GeaflowRuntimeException(String.format("cycle %d already added", cycle.getCycleId()));
        }
        cycleMap.put(cycle.getCycleId(), cycle);
        cycleParents.put(cycle.getCycleId(), new ArrayList<>());
        cycleChildren.put(cycle.getCycleId(), new ArrayList<>());

        ExecutionNodeCycle nodeCycle = (ExecutionNodeCycle) cycle;
        cycleParents.get(cycle.getCycleId()).addAll(nodeCycle.getVertexGroup().getParentVertexGroupIds());
        cycleChildren.get(cycle.getCycleId()).addAll(nodeCycle.getVertexGroup().getChildrenVertexGroupIds());

        if (!skipCheckpoint && (iterationCount > 1 && haLevel != HighAvailableLevel.CHECKPOINT
            && (cycle.getType() == ExecutionCycleType.ITERATION || cycle.getType() == ExecutionCycleType.ITERATION_WITH_AGG))) {
            haLevel = HighAvailableLevel.CHECKPOINT;
        }
    }

    public Map<Integer, IExecutionCycle> getCycleMap() {
        return cycleMap;
    }

    public Map<Integer, List<Integer>> getCycleChildren() {
        return cycleChildren;
    }

    public Map<Integer, List<Integer>> getCycleParents() {
        return cycleParents;
    }
}
