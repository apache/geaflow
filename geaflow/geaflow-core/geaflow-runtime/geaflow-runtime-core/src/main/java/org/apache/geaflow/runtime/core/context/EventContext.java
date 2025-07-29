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

package org.apache.geaflow.runtime.core.context;

import org.apache.geaflow.cluster.protocol.IEventContext;
import org.apache.geaflow.core.graph.ExecutionTask;
import org.apache.geaflow.processor.Processor;
import org.apache.geaflow.shuffle.IoDescriptor;

public class EventContext implements IEventContext {

    private long currentWindowId;
    private int taskId;
    private int cycleId;
    private long pipelineId;
    private String pipelineName;
    private Processor processor;
    private ExecutionTask executionTask;
    private IoDescriptor ioDescriptor;
    private String driverId;
    private long windowId;
    private long schedulerId;

    private EventContext(long schedulerId, long currentWindowId, int taskId, int cycleId, long pipelineId,
                         String pipelineName, Processor processor, ExecutionTask executionTask,
                         IoDescriptor ioDescriptor, String driverId, long windowId) {
        this.schedulerId = schedulerId;
        this.currentWindowId = currentWindowId;
        this.taskId = taskId;
        this.cycleId = cycleId;
        this.pipelineId = pipelineId;
        this.pipelineName = pipelineName;
        this.processor = processor;
        this.executionTask = executionTask;
        this.ioDescriptor = ioDescriptor;
        this.driverId = driverId;
        this.windowId = windowId;
    }

    public long getCurrentWindowId() {
        return currentWindowId;
    }

    public int getTaskId() {
        return taskId;
    }

    public int getCycleId() {
        return cycleId;
    }

    public long getPipelineId() {
        return pipelineId;
    }

    public String getPipelineName() {
        return pipelineName;
    }

    public Processor getProcessor() {
        return processor;
    }

    public ExecutionTask getExecutionTask() {
        return executionTask;
    }

    public IoDescriptor getIoDescriptor() {
        return ioDescriptor;
    }

    public String getDriverId() {
        return driverId;
    }

    public long getWindowId() {
        return windowId;
    }

    public long getSchedulerId() {
        return schedulerId;
    }

    public static EventContextBuilder builder() {
        return new EventContextBuilder();
    }

    public static class EventContextBuilder {
        private long currentWindowId;
        private int taskId;
        private int cycleId;
        private long pipelineId;
        private String pipelineName;
        private Processor processor;
        private ExecutionTask executionTask;
        private IoDescriptor ioDescriptor;
        private String driverId;
        private long windowId;
        private long schedulerId;

        public EventContextBuilder withExecutionTask(ExecutionTask executionTask) {
            this.executionTask = executionTask;
            this.processor = executionTask.getProcessor();
            this.taskId = executionTask.getTaskId();
            return this;
        }

        public EventContextBuilder withCurrentWindowId(long currentWindowId) {
            this.currentWindowId = currentWindowId;
            return this;
        }

        public EventContextBuilder withIoDescriptor(IoDescriptor ioDescriptor) {
            this.ioDescriptor = ioDescriptor;
            return this;
        }

        public EventContextBuilder withCycleId(int cycleId) {
            this.cycleId = cycleId;
            return this;
        }

        public EventContextBuilder withPipelineId(long pipelineId) {
            this.pipelineId = pipelineId;
            return this;
        }

        public EventContextBuilder withPipelineName(String pipelineName) {
            this.pipelineName = pipelineName;
            return this;
        }

        public EventContextBuilder withDriverId(String driverId) {
            this.driverId = driverId;
            return this;
        }

        public EventContextBuilder withWindowId(long windowId) {
            this.windowId = windowId;
            return this;
        }

        public EventContextBuilder withSchedulerId(long schedulerId) {
            this.schedulerId = schedulerId;
            return this;
        }

        public EventContext build() {
            return new EventContext(schedulerId, currentWindowId, taskId, cycleId, pipelineId,
                pipelineName, processor, executionTask, ioDescriptor, driverId, windowId);
        }
    }
}
