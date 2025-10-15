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

package org.apache.geaflow.common.metric;

import java.io.Serializable;

public class CycleMetrics implements Serializable {

    private String name;
    private String pipelineName;
    private String opName;

    private long duration;
    private long startTime;
    private long totalTasks;
    private int slowestTask;
    private long slowestTaskExecuteTime;
    private long inputRecords;
    private long inputKb;
    private long outputRecords;
    private long outputKb;
    private long avgGcTime;
    private long avgExecuteTime;

    public CycleMetrics(String name, String pipelineName, String opName) {
        this.name = name;
        this.pipelineName = pipelineName;
        this.opName = opName;
    }

    public String getName() {
        return name;
    }

    public String getPipelineName() {
        return pipelineName;
    }

    public String getOpName() {
        return opName;
    }

    public void setOpName(String opName) {
        this.opName = opName;
    }

    public long getDuration() {
        return duration;
    }

    public void setDuration(long duration) {
        this.duration = duration;
    }

    public long getStartTime() {
        return startTime;
    }

    public void setStartTime(long startTime) {
        this.startTime = startTime;
    }

    public long getTotalTasks() {
        return totalTasks;
    }

    public void setTotalTasks(long totalTasks) {
        this.totalTasks = totalTasks;
    }

    public int getSlowestTask() {
        return slowestTask;
    }

    public void setSlowestTask(int slowestTask) {
        this.slowestTask = slowestTask;
    }

    public long getAvgGcTime() {
        return avgGcTime;
    }

    public void setAvgGcTime(long avgGcTime) {
        this.avgGcTime = avgGcTime;
    }

    public long getAvgExecuteTime() {
        return avgExecuteTime;
    }

    public void setAvgExecuteTime(long avgExecuteTime) {
        this.avgExecuteTime = avgExecuteTime;
    }

    public long getSlowestTaskExecuteTime() {
        return slowestTaskExecuteTime;
    }

    public void setSlowestTaskExecuteTime(long slowestTaskExecuteTime) {
        this.slowestTaskExecuteTime = slowestTaskExecuteTime;
    }

    public long getInputRecords() {
        return inputRecords;
    }

    public void setInputRecords(long inputRecords) {
        this.inputRecords = inputRecords;
    }

    public long getOutputRecords() {
        return outputRecords;
    }

    public void setOutputRecords(long outputRecords) {
        this.outputRecords = outputRecords;
    }

    public long getInputKb() {
        return inputKb;
    }

    public void setInputKb(long inputKb) {
        this.inputKb = inputKb;
    }

    public long getOutputKb() {
        return outputKb;
    }

    public void setOutputKb(long outputKb) {
        this.outputKb = outputKb;
    }

    public static CycleMetrics build(String metricName,
                                     String pipelineName,
                                     String opName,
                                     int taskNum,
                                     int slowestTask,
                                     long startTime,
                                     long duration,
                                     long totalExecuteTime,
                                     long totalGcTime,
                                     long slowestTaskExecuteTime,
                                     long totalInputRecords,
                                     long totalInputBytes,
                                     long totalOutputRecords,
                                     long totalOutputBytes) {
        CycleMetrics cycleMetrics = new CycleMetrics(metricName, pipelineName, opName);
        cycleMetrics.setStartTime(startTime);
        cycleMetrics.setTotalTasks(taskNum);
        cycleMetrics.setSlowestTask(slowestTask);
        cycleMetrics.setDuration(duration);
        cycleMetrics.setAvgExecuteTime(totalExecuteTime / taskNum);
        cycleMetrics.setAvgGcTime(totalGcTime / taskNum);
        cycleMetrics.setSlowestTaskExecuteTime(slowestTaskExecuteTime);
        cycleMetrics.setInputRecords(totalInputRecords);
        cycleMetrics.setInputKb(totalInputBytes / 1024);
        cycleMetrics.setOutputRecords(totalOutputRecords);
        cycleMetrics.setOutputKb(totalOutputBytes / 1024);
        return cycleMetrics;
    }

    @Override
    public String toString() {
        return "CycleMetrics{"
            + "pipelineName='" + pipelineName + '\''
            + ", name='" + name + '\''
            + ", opName='" + opName + '\''
            + ", duration=" + duration + "ms"
            + ", totalTasks=" + totalTasks
            + ", slowestTask=" + slowestTask
            + ", slowestTaskExecuteTime=" + slowestTaskExecuteTime + "ms"
            + ", avgGcTime=" + avgGcTime + "ms"
            + ", avgExecuteTime=" + avgExecuteTime + "ms"
            + ", inputRecords=" + inputRecords
            + ", inputKb=" + inputKb
            + ", outputRecords=" + outputRecords
            + ", outputKb=" + outputKb
            + '}';
    }

}
