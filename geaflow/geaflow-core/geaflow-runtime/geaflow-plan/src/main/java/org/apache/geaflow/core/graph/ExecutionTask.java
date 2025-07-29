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

package org.apache.geaflow.core.graph;

import java.io.Serializable;
import org.apache.geaflow.cluster.resourcemanager.WorkerInfo;
import org.apache.geaflow.common.task.TaskArgs;
import org.apache.geaflow.common.utils.LoggerFormatter;
import org.apache.geaflow.processor.Processor;

public class ExecutionTask implements Serializable {

    private int taskId;
    private int index;
    private int vertexId;
    private int parallelism;
    private int maxParallelism;
    private int numPartitions;
    private WorkerInfo workerInfo;
    private Processor<?, ?> processor;
    private ExecutionTaskType executionTaskType;
    private boolean iterative;

    private long startTime;
    private long duration;
    private transient String taskName;

    public ExecutionTask(int taskId, int index, int parallelism, int maxParallelism, int numPartitions, int vertexId) {
        this.taskId = taskId;
        this.index = index;
        this.parallelism = parallelism;
        this.maxParallelism = maxParallelism;
        this.numPartitions = numPartitions;
        this.vertexId = vertexId;
        this.workerInfo = null;
        this.startTime = -1;
    }

    public int getTaskId() {
        return taskId;
    }

    public void setTaskId(int taskId) {
        this.taskId = taskId;
    }

    public int getIndex() {
        return index;
    }

    public void setIndex(int index) {
        this.index = index;
    }

    public int getParallelism() {
        return parallelism;
    }

    public void setParallelism(int parallelism) {
        this.parallelism = parallelism;
    }

    public int getMaxParallelism() {
        return maxParallelism;
    }

    public int getNumPartitions() {
        return numPartitions;
    }

    public int getVertexId() {
        return vertexId;
    }

    public void setVertexId(int vertexId) {
        this.vertexId = vertexId;
    }

    public WorkerInfo getWorkerInfo() {
        return workerInfo;
    }

    public void setWorkerInfo(WorkerInfo workerInfo) {
        this.workerInfo = workerInfo;
    }

    public long getStartTime() {
        return startTime;
    }

    public void setStartTime(long startTime) {
        this.startTime = startTime;
    }

    public long getDuration() {
        return duration;
    }

    public void setDuration(long duration) {
        this.duration = duration;
    }

    public Processor<?, ?> getProcessor() {
        return processor;
    }

    public void setProcessor(Processor<?, ?> processor) {
        this.processor = processor;
    }

    public ExecutionTaskType getExecutionTaskType() {
        return executionTaskType;
    }

    public void setExecutionTaskType(ExecutionTaskType executionTaskType) {
        this.executionTaskType = executionTaskType;
    }

    public void setIterative(boolean iterative) {
        this.iterative = iterative;
    }

    public boolean isIterative() {
        return this.iterative;
    }

    public String getTaskName() {
        return taskName;
    }

    public void buildTaskName(String pipelineName, int cycleId, long windowId) {
        this.taskName = this.iterative
            ? LoggerFormatter.getTaskTag(
            pipelineName, cycleId, windowId, this.taskId, this.vertexId, this.index, this.parallelism)
            : LoggerFormatter.getTaskTag(
            pipelineName, cycleId, this.taskId, this.vertexId, this.index, this.parallelism);
    }

    public TaskArgs buildTaskArgs() {
        return new TaskArgs(
            this.taskId,
            this.index,
            this.taskName,
            this.parallelism,
            this.maxParallelism,
            this.workerInfo.getProcessIndex());
    }

    @Override
    public String toString() {
        return "ExecutionTask{" + "taskId=" + taskId + ", index=" + index + ", vertexId="
            + vertexId + ", worker=" + (workerInfo == null ? "NULL" : workerInfo) + '}';
    }
}
