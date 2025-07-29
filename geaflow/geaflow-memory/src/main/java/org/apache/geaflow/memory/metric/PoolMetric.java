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

package org.apache.geaflow.memory.metric;

/**
 * This interface is the metrics for a pool.
 */
public interface PoolMetric {

    /**
     * Returns the number of thread caches backed by this arena.
     */
    int numThreadCaches();

    /**
     * Return the number of allocations done via the pool. This includes all sizes.
     */
    long numAllocations();

    /**
     * Returns the number of active bytes.
     */
    long numActiveBytes();

    /**
     * Returns the number of allocated bytes.
     */
    long allocateBytes();

    /**
     * Returns the number of free bytes.
     */
    long freeBytes();
}
