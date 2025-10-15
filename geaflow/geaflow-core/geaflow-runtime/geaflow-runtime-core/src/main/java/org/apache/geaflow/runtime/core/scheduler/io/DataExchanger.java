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

package org.apache.geaflow.runtime.core.scheduler.io;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.geaflow.cluster.response.IResult;
import org.apache.geaflow.cluster.response.ShardResult;
import org.apache.geaflow.core.graph.ExecutionEdge;
import org.apache.geaflow.core.graph.ExecutionVertex;
import org.apache.geaflow.shuffle.message.Shard;

public class DataExchanger {

    private static final ThreadLocal<Map<Integer, Map<Integer, List<Shard>>>> taskInputEdgeShards = ThreadLocal.withInitial(HashMap::new);

    /**
     * Build task input for execution vertex.
     *
     * @return key: taskIndex
     *         value: list of input shards
     */
    public static Map<Integer, List<Shard>> buildInput(ExecutionVertex vertex,
                                                       ExecutionEdge inputEdge,
                                                       CycleResultManager resultManager) {

        if (taskInputEdgeShards.get().containsKey(inputEdge.getEdgeId())) {
            return taskInputEdgeShards.get().get(inputEdge.getEdgeId());
        }
        Map<Integer, List<Shard>> result = new HashMap<>();

        int edgeId = inputEdge.getEdgeId();
        List<IResult> eventResults = resultManager.get(edgeId);
        Map<Integer, Shard> taskIdToInputShard = new HashMap<>();
        for (IResult eventResult : eventResults) {
            ShardResult shard = (ShardResult) eventResult;
            for (int i = 0; i < shard.getResponse().size(); i++) {
                int index = i % vertex.getParallelism();
                if (!taskIdToInputShard.containsKey(index)) {
                    taskIdToInputShard.put(index, new Shard(shard.getId(), new ArrayList<>()));
                }
                taskIdToInputShard.get(index).getSlices().add(shard.getResponse().get(i));
            }
        }
        for (Map.Entry<Integer, Shard> entry : taskIdToInputShard.entrySet()) {
            if (!result.containsKey(entry.getKey())) {
                result.put(entry.getKey(), new ArrayList<>());
            }
            result.get(entry.getKey()).add(entry.getValue());
        }
        taskInputEdgeShards.get().put(inputEdge.getEdgeId(), result);
        return result;
    }

    public static void clear() {
        taskInputEdgeShards.get().clear();
    }
}
