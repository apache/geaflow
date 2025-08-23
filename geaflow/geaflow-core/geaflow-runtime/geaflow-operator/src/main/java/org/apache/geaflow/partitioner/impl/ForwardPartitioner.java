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

package org.apache.geaflow.partitioner.impl;

import org.apache.geaflow.api.partition.IPartition;
import org.apache.geaflow.api.partition.kv.RandomPartition;

public class ForwardPartitioner<T> extends AbstractPartitioner<T> {

    public ForwardPartitioner() {
        super(-1);
    }

    public ForwardPartitioner(int opId) {
        super(opId);
    }

    @Override
    public IPartition<T> getPartition() {
        return new RandomPartition<T>();
    }

    @Override
    public PartitionType getPartitionType() {
        return PartitionType.forward;
    }

}
