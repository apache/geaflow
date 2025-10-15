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

package org.apache.geaflow.runtime.core.scheduler.context;

import java.util.function.Supplier;
import org.apache.geaflow.common.exception.GeaflowRuntimeException;
import org.apache.geaflow.runtime.core.scheduler.cycle.ExecutionCycleType;
import org.apache.geaflow.runtime.core.scheduler.cycle.IExecutionCycle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CycleSchedulerContextFactory {

    private static final Logger LOGGER = LoggerFactory.getLogger(CycleSchedulerContextFactory.class);

    public static <C extends IExecutionCycle, PC extends IExecutionCycle, PCC extends ICycleSchedulerContext<PC, ?, ?>>
        ICycleSchedulerContext<C, PC, PCC> loadOrCreate(long pipelineTaskId, Supplier<ICycleSchedulerContext<C, PC, PCC>> buildFunc) {
        return CheckpointSchedulerContext.build(pipelineTaskId, buildFunc);
    }

    public static ICycleSchedulerContext create(IExecutionCycle cycle, ICycleSchedulerContext parent) {
        AbstractCycleSchedulerContext context;
        switch (cycle.getHighAvailableLevel()) {
            case CHECKPOINT:
                LOGGER.info("create checkpoint scheduler context");
                context = new CheckpointSchedulerContext(cycle, parent);
                break;
            case REDO:
                if (cycle.getType() == ExecutionCycleType.ITERATION
                    || cycle.getType() == ExecutionCycleType.ITERATION_WITH_AGG) {
                    LOGGER.info("create iteration redo scheduler context");
                    context = new IterationRedoSchedulerContext(cycle, parent);
                } else {
                    LOGGER.info("create redo scheduler context");
                    context = new RedoSchedulerContext(cycle, parent);
                }
                break;
            default:
                throw new GeaflowRuntimeException(String.format("not support ha level %s for cycle %s",
                    cycle.getHighAvailableLevel(), cycle.getCycleId()));
        }
        context.init();
        return context;
    }
}
