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

package org.apache.geaflow.runtime.core.scheduler.strategy;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.stream.Collectors;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.common.exception.GeaflowRuntimeException;
import org.apache.geaflow.runtime.core.scheduler.cycle.ExecutionGraphCycle;
import org.apache.geaflow.runtime.core.scheduler.cycle.IExecutionCycle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TopologicalOrderScheduleStrategy implements IScheduleStrategy<ExecutionGraphCycle, IExecutionCycle> {

    private static final Logger LOGGER = LoggerFactory.getLogger(TopologicalOrderScheduleStrategy.class);

    private ExecutionGraphCycle graph;
    private LinkedBlockingDeque<IExecutionCycle> waiting;
    private LinkedBlockingDeque<IExecutionCycle> running;

    private Configuration config;

    // All stage ids that already finished.
    private Set<Integer> finishedIds;

    public TopologicalOrderScheduleStrategy(Configuration config) {
        this.config = config;
    }

    @Override
    public void init(ExecutionGraphCycle graph) {
        this.graph = graph;
        this.waiting = new LinkedBlockingDeque<>();
        this.running = new LinkedBlockingDeque<>();
        this.finishedIds = new HashSet<>();

        // Find head vertex.
        List<IExecutionCycle> heads = graph.getCycleParents().entrySet().stream()
            .filter(e -> e.getValue().isEmpty())
            .map(e -> graph.getCycleMap().get(e.getKey()))
            .sorted(Comparator.comparingInt(IExecutionCycle::getCycleId))
            .collect(Collectors.toList());
        // Add head to waiting list.
        waiting.addAll(heads);
    }

    @Override
    public boolean hasNext() {
        return !waiting.isEmpty();
    }

    @Override
    public IExecutionCycle next() {
        IExecutionCycle cycle = null;
        try {
            cycle = waiting.takeFirst();
        } catch (InterruptedException e) {
            throw new GeaflowRuntimeException("interrupted when waiting the cycle ready to schedule", e);
        }
        running.addLast(cycle);
        return cycle;
    }

    @Override
    public synchronized void finish(IExecutionCycle cycle) {
        finishedIds.add(cycle.getCycleId());
        // Recursively check and pop the head element of running stage queue if it finished.
        // To make sure that a stage the earlier added into running queue, the earlier removed.
        while (!running.isEmpty()
            && finishedIds.contains(running.peek().getCycleId())) {

            IExecutionCycle triggerCycle = running.remove();
            triggerChildren(triggerCycle);
        }
    }

    /**
     * Add the children to waiting list if necessary.
     */
    private void triggerChildren(IExecutionCycle cycle) {

        List<IExecutionCycle> readyToStartGroups = new ArrayList<>();

        for (int childId : graph.getCycleChildren().get(cycle.getCycleId())) {
            IExecutionCycle child = graph.getCycleMap().get(childId);
            boolean childParentAllDone = true;
            for (Integer childParentGroupId : graph.getCycleParents().get(childId)) {
                if (!finishedIds.contains(childParentGroupId)) {
                    childParentAllDone = false;
                    break;
                }
            }
            if (childParentAllDone) {
                readyToStartGroups.add(child);
            }
        }
        if (!readyToStartGroups.isEmpty()) {
            LOGGER.info("current waiting stages {}, new add stages {}",
                waiting.stream().map(e -> e.getCycleId()).collect(Collectors.toList()),
                readyToStartGroups.stream().map(e -> e.getCycleId()).collect(Collectors.toList()));
            for (IExecutionCycle group : readyToStartGroups) {
                addToWaiting(group);
            }
        }
    }

    /**
     * Add stage into waiting list.
     */
    private synchronized void addToWaiting(IExecutionCycle cycle) {
        // Avoid add a certain stage into waiting list multi-times.
        if (!waiting.stream().anyMatch(e -> e.getCycleId() == cycle.getCycleId())) {
            waiting.add(cycle);
        } else {
            LOGGER.info("cycle {} already added to waiting queue", cycle.getCycleId());
        }
    }

}

