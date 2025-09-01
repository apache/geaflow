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

import static org.apache.geaflow.runtime.core.scheduler.io.IoDescriptorBuilder.COLLECT_DATA_EDGE_ID;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.geaflow.cluster.response.IResult;

public class CycleResultManager {

    /**
     * edge id to result shard.
     */
    private Map<Integer, List<IResult>> shards;

    /**
     * all data response.
     */
    private List<IResult> rawDatas;

    public CycleResultManager() {
        this.shards = new ConcurrentHashMap<>();
        this.rawDatas = new ArrayList<>();
    }

    public void register(int id, IResult response) {
        if (!shards.containsKey(id)) {
            shards.put(id, new ArrayList<>());
        }
        shards.get(id).add(response);
    }

    public List<IResult> get(int id) {
        return shards.get(id);
    }

    public List<IResult> getDataResponse() {
        return shards.get(COLLECT_DATA_EDGE_ID);
    }

    public void release(int id) {
        shards.remove(id);
    }

    public void clear() {
        shards.clear();
    }
}