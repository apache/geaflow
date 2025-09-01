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

public class ProcessMetrics {

    /**
     * This amount of memory is guaranteed for the Java virtual machine to use.
     */
    private long heapCommittedMB;
    private long heapUsedMB;
    private double heapUsedRatio;
    private long totalMemoryMB;

    /**
     * the total number of full collections that have occurred.
     */
    private long fgcCount = 0L;
    /**
     * the total cost of full collections that have occurred.
     */
    private long fgcTime = 0L;
    /**
     * the approximate accumulated collection elapsed time in milliseconds.
     */
    private long gcTime = 0L;
    /**
     * the total number of collections that have occurred.
     */
    private long gcCount = 0;

    /**
     * the system load average for the last minute, or a negative value if not available.
     */
    private double avgLoad;
    /**
     * the number of processors available to the Java virtual machine.
     */
    private int availCores;
    /**
     * cpu usage.
     */
    private double processCpu;
    /**
     * the number of processors used.
     */
    private double usedCores;
    private int activeThreads;

    public long getHeapCommittedMB() {
        return heapCommittedMB;
    }

    public void setHeapCommittedMB(long heapCommittedMB) {
        this.heapCommittedMB = heapCommittedMB;
    }

    public long getHeapUsedMB() {
        return heapUsedMB;
    }

    public void setHeapUsedMB(long heapUsedMB) {
        this.heapUsedMB = heapUsedMB;
    }

    public double getHeapUsedRatio() {
        return heapUsedRatio;
    }

    public void setHeapUsedRatio(double heapUsedRatio) {
        this.heapUsedRatio = heapUsedRatio;
    }

    public long getTotalMemoryMB() {
        return totalMemoryMB;
    }

    public void setTotalMemoryMB(long totalMemoryMB) {
        this.totalMemoryMB = totalMemoryMB;
    }

    public long getFgcCount() {
        return fgcCount;
    }

    public void setFgcCount(long fgcCount) {
        this.fgcCount = fgcCount;
    }

    public long getFgcTime() {
        return fgcTime;
    }

    public void setFgcTime(long fgcTime) {
        this.fgcTime = fgcTime;
    }

    public long getGcTime() {
        return gcTime;
    }

    public void setGcTime(long gcTime) {
        this.gcTime = gcTime;
    }

    public long getGcCount() {
        return gcCount;
    }

    public void setGcCount(long gcCount) {
        this.gcCount = gcCount;
    }

    public double getAvgLoad() {
        return avgLoad;
    }

    public void setAvgLoad(double avgLoad) {
        this.avgLoad = avgLoad;
    }

    public int getAvailCores() {
        return availCores;
    }

    public void setAvailCores(int availCores) {
        this.availCores = availCores;
    }

    public double getUsedCores() {
        return usedCores;
    }

    public void setUsedCores(double usedCores) {
        this.usedCores = usedCores;
    }

    public double getProcessCpu() {
        return processCpu;
    }

    public void setProcessCpu(double processCpu) {
        this.processCpu = processCpu;
    }

    public int getActiveThreads() {
        return activeThreads;
    }

    public void setActiveThreads(int activeThreads) {
        this.activeThreads = activeThreads;
    }

    @Override
    public String toString() {
        return "ProcessMetrics{" + "heapCommittedMB=" + heapCommittedMB + ", heapUsedMB="
            + heapUsedMB + ", heapUsedRatio=" + heapUsedRatio + ", totalMemoryMB=" + totalMemoryMB
            + ", fgcCount=" + fgcCount + ", fgcTime=" + fgcTime + ", gcTime=" + gcTime
            + ", gcCount=" + gcCount + ", avgLoad=" + avgLoad + ", availCores=" + availCores
            + ", processCpu=" + processCpu + ", usedCores=" + usedCores + ", activeThreads="
            + activeThreads + '}';
    }

}
