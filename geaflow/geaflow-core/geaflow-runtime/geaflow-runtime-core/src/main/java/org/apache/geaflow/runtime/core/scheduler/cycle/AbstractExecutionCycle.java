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

import com.google.common.base.Preconditions;
import org.apache.geaflow.common.config.Configuration;

public abstract class AbstractExecutionCycle implements IExecutionCycle {

    protected long pipelineId;
    protected long pipelineTaskId;
    protected long schedulerId;
    protected String pipelineName;
    protected int cycleId;
    protected int flyingCount;
    protected long iterationCount;
    private Configuration config;

    public AbstractExecutionCycle(long schedulerId, long pipelineId, long pipelineTaskId, String pipelineName,
                                  int cycleId, int flyingCount, long iterationCount,
                                  Configuration config) {
        this.pipelineName = pipelineName;
        this.cycleId = cycleId;
        this.flyingCount = flyingCount;
        this.iterationCount = iterationCount;
        this.pipelineId = pipelineId;
        this.pipelineTaskId = pipelineTaskId;
        this.schedulerId = schedulerId;
        this.config = config;

        Preconditions.checkArgument(flyingCount > 0,
            "cycle flyingCount should be positive, current value %s", flyingCount);
        Preconditions.checkArgument(iterationCount > 0,
            "cycle iterationCount should be positive, current value %s", iterationCount);
    }


    public void setPipelineId(long pipelineId) {
        this.pipelineId = pipelineId;
    }

    public long getPipelineId() {
        return pipelineId;
    }

    public void setPipelineTaskId(long pipelineTaskId) {
        this.pipelineTaskId = pipelineTaskId;
    }

    @Override
    public long getPipelineTaskId() {
        return pipelineTaskId;
    }

    public void setSchedulerId(long schedulerId) {
        this.schedulerId = schedulerId;
    }

    @Override
    public long getSchedulerId() {
        return schedulerId;
    }

    public void setPipelineName(String pipelineName) {
        this.pipelineName = pipelineName;
    }

    @Override
    public String getPipelineName() {
        return pipelineName;
    }

    @Override
    public int getCycleId() {
        return cycleId;
    }

    @Override
    public int getFlyingCount() {
        return flyingCount;
    }

    @Override
    public long getIterationCount() {
        return iterationCount;
    }

    public Configuration getConfig() {
        return config;
    }
}
