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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;
import org.apache.geaflow.cluster.resourcemanager.WorkerInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProcessFairAllocator extends AbstractAllocator<String, WorkerInfo> {

    private static final Logger LOGGER = LoggerFactory.getLogger(ProcessFairAllocator.class);

    private static final String SEPARATOR = "-";

    @Override
    public List<WorkerInfo> doAllocate(int num) {

        int processNum = this.group2workers.size();
        if (num % processNum != 0) {
            LOGGER.warn("require num must be a multiple of process num, available {} require {}", this.group2workers.size(), num);
            return Collections.emptyList();
        }

        List<WorkerInfo> allocated = new ArrayList<>();
        int nPerProc = num / processNum;
        for (Map.Entry<Comparable<String>, LinkedList<WorkerInfo>> entry : this.group2workers.entrySet()) {
            String key = entry.getKey().toString();
            LinkedList<WorkerInfo> list = entry.getValue();
            if (list.size() < nPerProc) {
                LOGGER.warn("not enough worker for jvm {}, available {} require {}", key, list.size(), nPerProc);
                return Collections.emptyList();
            }
            IntStream.range(0, nPerProc).forEach(i -> allocated.add(list.pollFirst()));
        }

        // Sort workers and assemble process index id for every worker.
        assembleProcessIndexId(processNum, allocated);
        return allocated;
    }

    @Override
    public WorkerGroupByFunction<String, WorkerInfo> getWorkerGroupByFunction() {
        return PROC_GROUP_SELECTOR;
    }

    @Override
    public AllocateStrategy getStrategy() {
        return AllocateStrategy.PROCESS_FAIR;
    }

    /**
     * Sort workers and assemble process index id for every worker.
     *
     * @param workers
     */
    public void assembleProcessIndexId(int processNum, List<WorkerInfo> workers) {
        // Sort worker by hostname + process id in order to ensure sort by jvm.
        Collections.sort(workers, new Comparator<WorkerInfo>() {
            @Override
            public int compare(WorkerInfo o1, WorkerInfo o2) {
                return (o1.getHost() + SEPARATOR + o1.getProcessId()).compareTo(
                    o2.getHost() + SEPARATOR + o2.getProcessId());
            }
        });

        int step = workers.size() / processNum;
        int globalProcessIndex = 0;
        // Assemble the worker jvm index.
        for (int i = 0; i < workers.size(); i += step) {
            for (int j = 0; j < step; j++) {
                workers.get(i + j).setProcessIndex(globalProcessIndex);
            }
            globalProcessIndex++;
        }
    }

}
