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

package org.apache.geaflow.runtime.core.scheduler.cycle;

import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.ha.runtime.HighAvailableLevel;

public interface IExecutionCycle {

    /**
     * Returns pipeline name.
     */
    String getPipelineName();

    /**
     * Returns pipelineTask id.
     */
    long getPipelineTaskId();

    /**
     * Returns scheduler id.
     */
    long getSchedulerId();

    /**
     * Returns cycle id.
     */
    int getCycleId();

    /**
     * Returns flying count.
     */
    int getFlyingCount();

    /**
     * Returns iteration count.
     */
    long getIterationCount();

    /**
     * Returns config.
     */
    Configuration getConfig();

    /**
     * Returns execution cycle type.
     */
    ExecutionCycleType getType();

    /**
     * Returns driver id.
     */
    String getDriverId();

    /**
     * Returns driver index.
     */
    int getDriverIndex();

    /**
     * Returns HA level.
     */
    HighAvailableLevel getHighAvailableLevel();

}
