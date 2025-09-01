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

import java.util.List;
import org.apache.geaflow.cluster.resourcemanager.WorkerInfo;
import org.apache.geaflow.runtime.core.scheduler.cycle.IExecutionCycle;

public interface IScheduledWorkerManager<G> {

    /**
     * Init the worker manager by input graph.
     * The graph info will help to decide the total worker resources required.
     */
    void init(G graph);

    /**
     * Assign workers for execution task of input graph.
     *
     * @return Workers if assign worker succeed, otherwise empty.
     */
    List<WorkerInfo> assign(G graph);

    /**
     * Release all worker resource for the input graph.
     */
    void release(G graph);

    /**
     * Clean worker runtime context for used workers by specified clean function.
     */
    void clean(CleanWorkerFunction cleaFunc, IExecutionCycle cycle);

    /**
     * Release all worker to master resource manager.
     */
    void close(IExecutionCycle cycle);

    /**
     * Function interface to clean runtime context for already assigned workers.
     */
    interface CleanWorkerFunction {

        void clean(List<WorkerInfo> assignedWorkers);

    }
}
