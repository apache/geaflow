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

package org.apache.geaflow.api.partition.kv;

import org.apache.geaflow.api.partition.IPartition;
import org.apache.geaflow.utils.keygroup.KeyGroupAssignment;

public class KeyByPartition<K> implements IPartition<K> {

    private int maxParallelism;

    public KeyByPartition(int maxParallelism) {
        this.maxParallelism = maxParallelism;
    }

    @Override
    public int[] partition(K key, int numPartition) {
        int channel = KeyGroupAssignment.assignKeyToParallelTask(key, maxParallelism, numPartition);
        return new int[]{channel};
    }


}
