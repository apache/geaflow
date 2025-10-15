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

package org.apache.geaflow.runtime.core.scheduler;

import org.apache.geaflow.runtime.core.scheduler.context.ICycleSchedulerContext;
import org.apache.geaflow.runtime.core.scheduler.cycle.IExecutionCycle;
import org.apache.geaflow.runtime.core.scheduler.result.IExecutionResult;

public interface ICycleScheduler<
    C extends IExecutionCycle,
    PC extends IExecutionCycle,
    PCC extends ICycleSchedulerContext<PC, ?, ?>,
    R, E> {

    /**
     * Initialize cycle scheduler by input context.
     * May include assign resource and initialize worker, and set up cycle schedule env.
     */
    void init(ICycleSchedulerContext<C, PC, PCC> context);

    /**
     * Execution all cycles.
     */
    IExecutionResult<R, E> execute();

    /**
     * Close the initialized resources and workers.
     */
    void close();

}
