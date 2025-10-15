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

package org.apache.geaflow.cluster.resourcemanager.allocator;

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.apache.geaflow.cluster.resourcemanager.WorkerInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractAllocator<G, W> implements IAllocator<G, W> {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractAllocator.class);

    protected static final WorkerGroupByFunction<String, WorkerInfo> PROC_GROUP_SELECTOR =
        worker -> String.format("%s-%d", worker.getHost(), worker.getProcessId());

    protected Map<Comparable<G>, LinkedList<W>> group2workers;

    protected AbstractAllocator() {
        this.group2workers = new TreeMap<>();
    }

    @Override
    public List<W> allocate(Collection<W> idleWorkers, int num) {

        if (idleWorkers.size() < num) {
            LOGGER.warn("worker not enough, available {} require {}", idleWorkers.size(), num);
            return Collections.emptyList();
        }

        WorkerGroupByFunction<G, W> groupSelector = this.getWorkerGroupByFunction();
        for (W worker : idleWorkers) {
            Comparable<G> group = groupSelector.getGroup(worker);
            List<W> list = this.group2workers.computeIfAbsent(group, g -> new LinkedList<>());
            list.add(worker);
        }

        List<W> allocated = doAllocate(num);
        reset();

        return allocated;
    }

    /**
     * Allocate workers with strategy.
     *
     * @param num number
     * @return workers
     */
    protected abstract List<W> doAllocate(int num);

    private void reset() {
        this.group2workers.clear();
    }

}
