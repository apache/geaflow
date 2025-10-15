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

package org.apache.geaflow.stats.collector;

import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.lang.management.OperatingSystemMXBean;
import java.lang.management.ThreadMXBean;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.commons.io.FileUtils;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.common.metric.ProcessMetrics;
import org.apache.geaflow.common.utils.ProcessUtil;
import org.apache.geaflow.metrics.common.MetricConstants;
import org.apache.geaflow.metrics.common.MetricGroupRegistry;
import org.apache.geaflow.metrics.common.MetricNameFormatter;
import org.apache.geaflow.metrics.common.api.Counter;
import org.apache.geaflow.metrics.common.api.Histogram;
import org.apache.geaflow.metrics.common.api.MetricGroup;

public class ProcessStatsCollector {

    private static final List<String> OLD_GEN_COLLECTOR_NAMES = Arrays.asList(
        // Oracle (Sun) HotSpot
        // -XX:+UseSerialGC
        "MarkSweepCompact",
        // -XX:+UseParallelGC and (-XX:+UseParallelOldGC or -XX:+UseParallelOldGCCompacting)
        "PS MarkSweep",
        // -XX:+UseConcMarkSweepGC
        "ConcurrentMarkSweep",

        // Oracle (BEA) JRockit
        // -XgcPrio:pausetime
        "Garbage collection optimized for short pausetimes Old Collector",
        // -XgcPrio:throughput
        "Garbage collection optimized for throughput Old Collector",
        // -XgcPrio:deterministic
        "Garbage collection optimized for deterministic pausetimes Old Collector",
        //UseG1GC
        "G1 Old Generation");

    private long preTimeNano = System.nanoTime();
    private long preCpuTimeNano = -1;
    private Map<Long, Long> threadMap = new ConcurrentHashMap<>();

    private final Counter totalUsedHeapMB;
    private final Counter totalMemoryMB;
    private final Histogram usedHeapRatio;
    private final Histogram gcTimeHistogram;
    private final Histogram fgcTimeHistogram;
    private final Histogram fgcCountHistogram;

    ProcessStatsCollector(Configuration configuration) {
        MetricGroupRegistry metricGroupRegistry = MetricGroupRegistry.getInstance(configuration);
        MetricGroup metricGroup = metricGroupRegistry.getMetricGroup(MetricConstants.MODULE_SYSTEM);
        totalUsedHeapMB = metricGroup.counter(MetricNameFormatter.totalHeapMetricName());
        totalMemoryMB = metricGroup.counter(MetricNameFormatter.totalMemoryMetricName());
        usedHeapRatio = metricGroup.histogram(MetricNameFormatter.heapUsageRatioMetricName());
        gcTimeHistogram = metricGroup.histogram(MetricNameFormatter.gcTimeMetricName());
        fgcTimeHistogram = metricGroup.histogram(MetricNameFormatter.fgcCountMetricName());
        fgcCountHistogram = metricGroup.histogram(MetricNameFormatter.fgcTimeMetricName());
    }

    public ProcessMetrics collect() {

        MemoryMXBean memoryMXBean = ManagementFactory.getMemoryMXBean();
        MemoryUsage heapMemory = memoryMXBean.getHeapMemoryUsage();
        long committedMB = heapMemory.getCommitted() / FileUtils.ONE_MB;
        long usedMB = heapMemory.getUsed() / FileUtils.ONE_MB;

        ProcessMetrics workerMetrics = new ProcessMetrics();
        workerMetrics.setHeapCommittedMB(committedMB);
        workerMetrics.setHeapUsedMB(usedMB);
        workerMetrics.setTotalMemoryMB(ProcessUtil.getTotalMemory());

        if (committedMB != 0) {
            double percentage = Math.round(usedMB * 100.0 / committedMB);
            workerMetrics.setHeapUsedRatio(percentage);
        }

        long fgcCount = 0L;
        long fgcTime = 0L;
        long gcTime = 0L;
        long gcCount = 0;

        List<GarbageCollectorMXBean> mxBeans = ManagementFactory.getGarbageCollectorMXBeans();
        for (GarbageCollectorMXBean gmx : mxBeans) {
            gcCount += gmx.getCollectionCount();
            gcTime += gmx.getCollectionTime();
            if (OLD_GEN_COLLECTOR_NAMES.contains(gmx.getName())) {
                fgcCount += gmx.getCollectionCount();
                fgcTime += gmx.getCollectionTime();
            }
        }

        workerMetrics.setFgcCount(fgcCount);
        workerMetrics.setFgcTime(fgcTime);
        workerMetrics.setGcTime(gcTime);
        workerMetrics.setGcCount(gcCount);

        OperatingSystemMXBean systemMXBean = ManagementFactory.getOperatingSystemMXBean();
        String avgLoad = String.format("%.2f", systemMXBean.getSystemLoadAverage());
        workerMetrics.setAvgLoad(Double.parseDouble(avgLoad));

        int availProcessors = systemMXBean.getAvailableProcessors();
        workerMetrics.setAvailCores(availProcessors);

        double cpuUsage = getCpuUsage();
        double avgCpuUsage = cpuUsage / availProcessors;
        workerMetrics.setProcessCpu(Double.parseDouble(String.format("%.2f", avgCpuUsage)));
        workerMetrics.setUsedCores(Double.parseDouble(String.format("%.2f", cpuUsage / 100.0)));
        workerMetrics.setActiveThreads(threadMap.size());
        uploadMetrics(workerMetrics);

        return workerMetrics;
    }

    private synchronized double getCpuUsage() {
        ThreadMXBean threadMXBean = ManagementFactory.getThreadMXBean();

        // total CPU time for threads in nanoseconds.
        long totalTimeNano = 0;
        Map<Long, Long> currentThreadMap = new HashMap<>();
        for (long id : threadMXBean.getAllThreadIds()) {
            long threadCpuTime = threadMXBean.getThreadCpuTime(id);
            if (threadCpuTime > 0) {
                totalTimeNano += threadCpuTime;
                currentThreadMap.put(id, threadCpuTime);
                threadMap.remove(id);
            }
        }
        for (Map.Entry<Long, Long> entry : threadMap.entrySet()) {
            totalTimeNano += entry.getValue();
        }
        threadMap.clear();
        threadMap = currentThreadMap;

        long curTimeNano = System.nanoTime();
        long usedCpuTime = preCpuTimeNano == -1 ? 0 : totalTimeNano - preCpuTimeNano;
        long totalPassedTime = curTimeNano - preTimeNano;
        preTimeNano = curTimeNano;
        preCpuTimeNano = totalTimeNano;

        return usedCpuTime * 100.0 / totalPassedTime;
    }

    private void uploadMetrics(ProcessMetrics metrics) {
        totalUsedHeapMB.inc(metrics.getHeapUsedMB());
        totalMemoryMB.inc(metrics.getTotalMemoryMB());

        gcTimeHistogram.update(metrics.getGcTime());
        fgcTimeHistogram.update(metrics.getFgcTime());
        fgcCountHistogram.update(metrics.getFgcCount());
        usedHeapRatio.update((int) (metrics.getHeapUsedRatio()));
    }

}
