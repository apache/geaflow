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

import org.apache.geaflow.ha.runtime.HighAvailableLevel;
import org.apache.geaflow.runtime.core.scheduler.cycle.IExecutionCycle;

public class RedoSchedulerContext<
    C extends IExecutionCycle,
    PC extends IExecutionCycle,
    PCC extends ICycleSchedulerContext<PC, ?, ?>> extends AbstractCycleSchedulerContext<C, PC, PCC> {

    public RedoSchedulerContext(C cycle, PCC parentContext) {
        super(cycle, parentContext);
    }

    @Override
    public void init() {
        super.init();
    }

    @Override
    protected HighAvailableLevel getHaLevel() {
        return HighAvailableLevel.REDO;
    }

    @Override
    public void checkpoint(long iterationId) {
        lastCheckpointId = iterationId;
    }
}
