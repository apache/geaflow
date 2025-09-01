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

package org.apache.geaflow.cluster.resourcemanager;

import org.apache.geaflow.cluster.resourcemanager.allocator.IAllocator;

public class RequireResourceRequest {

    private final String requireId;
    private final int requiredNum;
    private final IAllocator.AllocateStrategy allocateStrategy;

    private RequireResourceRequest(String requireId,
                                   int requiredNum,
                                   IAllocator.AllocateStrategy allocateStrategy) {
        this.requireId = requireId;
        this.requiredNum = requiredNum;
        this.allocateStrategy = allocateStrategy;
    }

    public String getRequireId() {
        return this.requireId;
    }

    public int getRequiredNum() {
        return this.requiredNum;
    }

    public IAllocator.AllocateStrategy getAllocateStrategy() {
        return this.allocateStrategy;
    }

    public static RequireResourceRequest build(String requireId, int requiredNum) {
        return new RequireResourceRequest(requireId, requiredNum, IAllocator.DEFAULT_ALLOCATE_STRATEGY);
    }

    public static RequireResourceRequest build(String requireId, int requiredNum, IAllocator.AllocateStrategy allocateStrategy) {
        return new RequireResourceRequest(requireId, requiredNum, allocateStrategy);
    }

}
