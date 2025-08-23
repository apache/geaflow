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
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import org.apache.geaflow.cluster.resourcemanager.WorkerInfo;

public class RoundRobinAllocator extends AbstractAllocator<String, WorkerInfo> {

    @Override
    public List<WorkerInfo> doAllocate(int num) {

        List<WorkerInfo> allocated = new ArrayList<>();
        int n = 0;
        Iterator<Comparable<String>> groupIterator = this.group2workers.keySet().iterator();
        while (n < num) {
            if (groupIterator.hasNext()) {
                Comparable<String> group = groupIterator.next();
                LinkedList<WorkerInfo> next = this.group2workers.get(group);
                if (!next.isEmpty()) {
                    allocated.add(next.pollFirst());
                    n++;
                }
            } else {
                groupIterator = this.group2workers.keySet().iterator();
            }
        }

        return allocated;
    }

    @Override
    public AllocateStrategy getStrategy() {
        return AllocateStrategy.ROUND_ROBIN;
    }

    @Override
    public WorkerGroupByFunction<String, WorkerInfo> getWorkerGroupByFunction() {
        return PROC_GROUP_SELECTOR;
    }

}

