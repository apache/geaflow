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

package org.apache.geaflow.runtime.core.worker.context;

import java.util.List;
import org.apache.geaflow.api.context.RuntimeContext;
import org.apache.geaflow.cluster.protocol.IEventContext;
import org.apache.geaflow.cluster.task.ITaskContext;
import org.apache.geaflow.cluster.worker.IWorkerContext;
import org.apache.geaflow.collector.ICollector;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.common.metric.EventMetrics;
import org.apache.geaflow.core.graph.ExecutionTask;
import org.apache.geaflow.metrics.common.api.MetricGroup;
import org.apache.geaflow.processor.Processor;
import org.apache.geaflow.runtime.core.context.DefaultRuntimeContext;
import org.apache.geaflow.runtime.core.context.EventContext;
import org.apache.geaflow.shuffle.IoDescriptor;

public abstract class AbstractWorkerContext implements IWorkerContext {

    protected EventContext eventContext;
    protected long currentWindowId;
    protected int taskId;
    protected Processor processor;
    protected Configuration config;
    protected boolean enableDebug;

    protected ExecutionTask executionTask;
    protected IoDescriptor ioDescriptor;
    protected int cycleId;
    protected long pipelineId;
    protected long schedulerId;
    protected String pipelineName;
    protected String driverId;
    protected MetricGroup metricGroup;
    protected EventMetrics eventMetrics;
    protected List<ICollector<?>> collectors;
    protected long windowId;
    protected RuntimeContext runtimeContext;
    protected boolean isFinished;

    public AbstractWorkerContext(ITaskContext taskContext) {
        this.config = taskContext.getConfig();
        this.metricGroup = taskContext.getMetricGroup();
        this.enableDebug = false;
    }

    @Override
    public void init(IEventContext eventContext) {
        this.eventContext = (EventContext) eventContext;
        this.currentWindowId = this.eventContext.getCurrentWindowId();
        this.cycleId = this.eventContext.getCycleId();
        this.pipelineId = this.eventContext.getPipelineId();
        this.schedulerId = this.eventContext.getSchedulerId();
        this.pipelineName = this.eventContext.getPipelineName();
        this.driverId = this.eventContext.getDriverId();
        this.ioDescriptor = this.eventContext.getIoDescriptor();
        this.executionTask = this.eventContext.getExecutionTask();
        this.processor = this.executionTask.getProcessor();
        this.taskId = this.executionTask.getTaskId();
        this.windowId = this.eventContext.getWindowId();
        this.runtimeContext = createRuntimeContext();
        this.isFinished = false;
        this.initEventMetrics();
    }

    public EventContext getEventContext() {
        return this.eventContext;
    }

    /**
     * Create runtime context and set io descriptor.
     */
    private RuntimeContext createRuntimeContext() {
        return DefaultRuntimeContext.build(config)
            .setTaskArgs(this.executionTask.buildTaskArgs())
            .setPipelineId(pipelineId)
            .setPipelineName(pipelineName)
            .setMetricGroup(metricGroup)
            .setIoDescriptor(ioDescriptor)
            .setWindowId(windowId);
    }

    public long getCurrentWindowId() {
        return currentWindowId;
    }

    public void setCurrentWindowId(long currentWindowId) {
        this.currentWindowId = currentWindowId;
    }

    public String getDriverId() {
        return driverId;
    }

    public void setTaskId(int taskId) {
        this.taskId = taskId;
    }

    public int getTaskId() {
        return taskId;
    }

    public int getCycleId() {
        return cycleId;
    }

    public Processor getProcessor() {
        return processor;
    }

    public ExecutionTask getExecutionTask() {
        return executionTask;
    }

    public EventMetrics getEventMetrics() {
        return eventMetrics;
    }

    public List<ICollector<?>> getCollectors() {
        return collectors;
    }

    public void setCollectors(List<ICollector<?>> collectors) {
        this.collectors = collectors;
    }

    public long getPipelineId() {
        return pipelineId;
    }

    public String getPipelineName() {
        return pipelineName;
    }

    public void setPipelineId(long pipelineId) {
        this.pipelineId = pipelineId;
    }

    public void setSchedulerId(long schedulerId) {
        this.schedulerId = schedulerId;
    }

    public long getSchedulerId() {
        return schedulerId;
    }

    public void setPipelineName(String pipelineName) {
        this.pipelineName = pipelineName;
    }

    public void setWindowId(long windowId) {
        this.windowId = windowId;
    }

    public long getWindowId() {
        return windowId;
    }

    public RuntimeContext getRuntimeContext() {
        return runtimeContext;
    }

    public boolean isIterativeTask() {
        return this.executionTask.isIterative();
    }

    public boolean isFinished() {
        return isFinished;
    }

    public void setFinished(boolean finished) {
        isFinished = finished;
    }

    public void initEventMetrics() {
        this.eventMetrics = new EventMetrics(
            this.executionTask.getVertexId(),
            this.executionTask.getParallelism(),
            this.executionTask.getIndex());
    }

}
