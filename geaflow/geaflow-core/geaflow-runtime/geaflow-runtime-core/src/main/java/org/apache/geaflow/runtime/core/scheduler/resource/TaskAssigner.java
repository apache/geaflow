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

package org.apache.geaflow.runtime.core.scheduler.resource;

import static org.apache.geaflow.runtime.core.scheduler.resource.AbstractScheduledWorkerManager.DEFAULT_GRAPH_VIEW_NAME;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import org.apache.geaflow.cluster.resourcemanager.WorkerInfo;

public class TaskAssigner implements Serializable {

    private Map<String, Map<Integer, Set<WorkerInfo>>> historyBindings;

    public TaskAssigner() {
        if (this.historyBindings == null) {
            this.historyBindings = new HashMap<>();
        }
    }

    /**
     * The matching of tasks and workers is a bipartite graph matching problem. The Task
     * Assigner use the Hopcroft–Karp algorithm for bipartite graph matching, with a time
     * complexity of O(e√v), where e is the number of edges and v is the number of vertices.
     *
     * @return the matching of tasks and workers.
     */
    public Map<Integer, WorkerInfo> assignTasks2Workers(String graphName, List<Integer> tasks,
                                                        List<WorkerInfo> workers) {
        if (tasks.size() != workers.size()) {
            throw new IllegalArgumentException(
                "Tasks and workers queues must have the same length.");
        }
        if (DEFAULT_GRAPH_VIEW_NAME.equals(graphName)) {
            Map<Integer, WorkerInfo> matches = new HashMap<>();
            Iterator<WorkerInfo> workerIterator = workers.iterator();
            for (Integer task : tasks) {
                if (workerIterator.hasNext()) {
                    WorkerInfo worker = workerIterator.next();
                    matches.put(task, worker);
                }
            }
            return matches;
        }

        Map<Integer, Set<WorkerInfo>> historyBinding = historyBindings.computeIfAbsent(graphName,
            k -> new HashMap<>());
        // Step 1: Build bipartite graph based on history.
        Map<Integer, List<WorkerInfo>> graph = new HashMap<>();
        for (
            Integer task : tasks) {
            graph.put(task, new ArrayList<>());
            if (historyBinding.containsKey(task)) {
                for (WorkerInfo worker : historyBinding.get(task)) {
                    if (workers.contains(worker)) {
                        graph.get(task).add(worker);
                    }
                }
            }
        }

        // Step 2: Use Hopcroft-Karp to find maximum matching.
        Map<Integer, WorkerInfo> matches = hopcroftKarp(tasks, workers, graph);

        // Step 3: Assign remaining workers to tasks without a match.
        Set<Integer> unmatchedTasks = new HashSet<>(tasks);
        unmatchedTasks.removeAll(matches.keySet());
        Set<WorkerInfo> unmatchedWorkers = new HashSet<>(workers);
        unmatchedWorkers.removeAll(matches.values());

        Iterator<WorkerInfo> workerIterator = unmatchedWorkers.iterator();
        for (Integer task : unmatchedTasks) {
            if (workerIterator.hasNext()) {
                WorkerInfo worker = workerIterator.next();
                matches.put(task, worker);
            }
        }

        // Update history bindings.
        for (Map.Entry<Integer, WorkerInfo> entry : matches.entrySet()) {
            Integer task = entry.getKey();
            WorkerInfo worker = entry.getValue();
            historyBinding.putIfAbsent(task, new HashSet<>());
            historyBinding.get(task).add(worker);
        }
        return matches;
    }

    private Map<Integer, WorkerInfo> hopcroftKarp(List<Integer> tasks, List<WorkerInfo> workers,
                                                  Map<Integer, List<WorkerInfo>> graph) {
        Map<Integer, WorkerInfo> pairU = new HashMap<>();
        Map<WorkerInfo, Integer> pairV = new HashMap<>();
        Map<Integer, Integer> dist = new HashMap<>();

        for (Integer task : tasks) {
            pairU.put(task, null);
        }
        for (WorkerInfo worker : workers) {
            pairV.put(worker, null);
        }

        int inf = Integer.MAX_VALUE;

        while (bfs(tasks, pairU, pairV, dist, graph, inf)) {
            for (Integer task : tasks) {
                if (pairU.get(task) == null) {
                    // DFS will update pairU and pairV.
                    dfs(task, pairU, pairV, dist, graph, inf);
                }
            }
        }

        Map<Integer, WorkerInfo> matches = new HashMap<>();
        for (Integer task : pairU.keySet()) {
            if (pairU.get(task) != null) {
                matches.put(task, pairU.get(task));
            }
        }

        return matches;
    }

    private boolean bfs(List<Integer> tasks, Map<Integer, WorkerInfo> pairU,
                        Map<WorkerInfo, Integer> pairV, Map<Integer, Integer> dist,
                        Map<Integer, List<WorkerInfo>> graph, int inf) {
        Queue<Integer> queue = new LinkedList<>();

        for (Integer task : tasks) {
            if (pairU.get(task) == null) {
                dist.put(task, 0);
                queue.add(task);
            } else {
                dist.put(task, inf);
            }
        }

        boolean hasAugmentingPath = false;

        while (!queue.isEmpty()) {
            Integer task = queue.poll();
            if (dist.get(task) < inf) {
                for (WorkerInfo worker : graph.get(task)) {
                    Integer nextTask = pairV.get(worker);
                    if (nextTask == null) {
                        hasAugmentingPath = true;
                    } else if (dist.get(nextTask) == inf) {
                        dist.put(nextTask, dist.get(task) + 1);
                        queue.add(nextTask);
                    }
                }
            }
        }

        return hasAugmentingPath;
    }

    private boolean dfs(Integer task, Map<Integer, WorkerInfo> pairU,
                        Map<WorkerInfo, Integer> pairV, Map<Integer, Integer> dist,
                        Map<Integer, List<WorkerInfo>> graph, int inf) {
        if (task != null) {
            for (WorkerInfo worker : graph.get(task)) {
                Integer nextTask = pairV.get(worker);
                if (nextTask == null || (dist.get(nextTask) == dist.get(task) + 1 && dfs(nextTask,
                    pairU, pairV, dist, graph, inf))) {
                    pairV.put(worker, task);
                    pairU.put(task, worker);
                    return true;
                }
            }
            dist.put(task, inf);
            return false;
        }
        return true;
    }
}
