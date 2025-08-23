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

package org.apache.geaflow.metrics.common;

import com.codahale.metrics.MetricRegistry;

public class MetricNameFormatter {

    //////////////////////////////
    // System

    /// ///////////////////////////

    public static String totalHeapMetricName() {
        return MetricConstants.METRIC_TOTAL_HEAP + MetricConstants.UNIT_N;
    }

    public static String totalMemoryMetricName() {
        return MetricConstants.METRIC_TOTAL_MEMORY + MetricConstants.UNIT_N;
    }

    public static String heapUsageRatioMetricName() {
        return MetricConstants.METRIC_HEAP_USAGE_RATIO + MetricConstants.UNIT_N;
    }

    public static String gcTimeMetricName() {
        return MetricConstants.METRIC_GC_TIME + MetricConstants.UNIT_MS;
    }

    public static String fgcCountMetricName() {
        return MetricConstants.METRIC_FGC_COUNT + MetricConstants.UNIT_N;
    }

    public static String fgcTimeMetricName() {
        return MetricConstants.METRIC_FGC_TIME + MetricConstants.UNIT_MS;
    }

    //////////////////////////////
    // Framework

    /// ///////////////////////////

    public static String inputTpsMetricName(Class<?> opClass, int opId) {
        String metricName = String.format("%s[%d]%s", opClass.getSimpleName(), opId, MetricConstants.UNIT_N);
        return MetricRegistry.name(MetricConstants.METRIC_INPUT_TPS, metricName);
    }

    public static String outputTpsMetricName(Class<?> opClass, int opId) {
        String metricName = String.format("%s[%d]%s", opClass.getSimpleName(), opId, MetricConstants.UNIT_N);
        return MetricRegistry.name(MetricConstants.METRIC_OUTPUT_TPS, metricName);
    }

    public static String rtMetricName(Class<?> opClass, int opId) {
        String metricName = String.format("%s[%d]%s", opClass.getSimpleName(), opId, MetricConstants.UNIT_US);
        return MetricRegistry.name(MetricConstants.METRIC_PROCESS_RT, metricName);
    }

    public static String vertexTpsMetricName(Class<?> opClass, int opId) {
        String metricName = String.format("%s[%d]%s", opClass.getSimpleName(), opId, MetricConstants.UNIT_N);
        return MetricRegistry.name(MetricConstants.METRIC_VERTEX_TPS, metricName);
    }

    public static String edgeTpsMetricName(Class<?> opClass, int opId) {
        String metricName = String.format("%s[%d]%s", opClass.getSimpleName(), opId, MetricConstants.UNIT_N);
        return MetricRegistry.name(MetricConstants.METRIC_EDGE_TPS, metricName);
    }

    public static String iterationFinishMetricName(Class<?> opClass, int opId, long iterationId) {
        String metricName = String.format("%s[%d:%d]%s", opClass.getSimpleName(), opId, iterationId, MetricConstants.UNIT_MS);
        return MetricRegistry.name(MetricConstants.METRIC_ITERATION, metricName);
    }

    public static String iterationMsgMetricName(Class<?> opClass, int opId) {
        String metricName = String.format("%s[%d]%s", opClass.getSimpleName(), opId, MetricConstants.UNIT_N);
        return MetricRegistry.name(MetricConstants.METRIC_ITERATION_MSG_TPS, metricName);
    }

    public static String iterationAggMetricName(Class<?> opClass, int opId) {
        String metricName = String.format("%s[%d]%s", opClass.getSimpleName(), opId, MetricConstants.UNIT_N);
        return MetricRegistry.name(MetricConstants.METRIC_ITERATION_AGG_TPS, metricName);
    }

    //////////////////////////////
    // Dsl

    /// ///////////////////////////

    public static String tableInputRowName(String name) {
        String metricName = String.format("%s%s", name, MetricConstants.UNIT_N);
        return MetricRegistry.name(MetricConstants.METRIC_TABLE_INPUT_ROW, metricName);
    }

    public static String stepInputRecordName(String name) {
        String metricName = String.format("%s%s", name, MetricConstants.UNIT_N);
        return MetricRegistry.name(MetricConstants.METRIC_STEP_INPUT_RECORD, metricName);
    }

    public static String stepOutputRecordName(String name) {
        String metricName = String.format("%s%s", name, MetricConstants.UNIT_N);
        return MetricRegistry.name(MetricConstants.METRIC_STEP_OUTPUT_RECORD, metricName);
    }

    public static String stepInputEodName(String name) {
        String metricName = String.format("%s%s", name, MetricConstants.UNIT_N);
        return MetricRegistry.name(MetricConstants.METRIC_STEP_INPUT_EOD, metricName);
    }

    public static String tableOutputRowTpsName(String name) {
        String metricName = String.format("%s%s", name, MetricConstants.UNIT_ROW_PER_S);
        return MetricRegistry.name(MetricConstants.METRIC_TABLE_OUTPUT_ROW_TPS, metricName);
    }

    public static String tableInputRowTpsName(String tableName) {
        String metricName = String.format("%s%s", tableName, MetricConstants.UNIT_ROW_PER_S);
        return MetricRegistry.name(MetricConstants.METRIC_TABLE_INPUT_ROW_TPS, metricName);
    }

    public static String tableInputBlockTpsName(String name) {
        String metricName = String.format("%s%s", name, MetricConstants.UNIT_BLOCK_PER_S);
        return MetricRegistry.name(MetricConstants.METRIC_TABLE_INPUT_BLOCK_TPS, metricName);
    }

    public static String stepInputRowTpsName(String name) {
        String metricName = String.format("%s%s", name, MetricConstants.UNIT_ROW_PER_S);
        return MetricRegistry.name(MetricConstants.METRIC_STEP_INPUT_ROW_TPS, metricName);
    }

    public static String stepOutputRowTpsName(String name) {
        String metricName = String.format("%s%s", name, MetricConstants.UNIT_ROW_PER_S);
        return MetricRegistry.name(MetricConstants.METRIC_STEP_OUTPUT_ROW_TPS, metricName);
    }

    public static String tableWriteTimeRtName(String name) {
        String metricName = String.format("%s%s", name, MetricConstants.UNIT_MS);
        return MetricRegistry.name(MetricConstants.METRIC_TABLE_WRITE_TIME_RT, metricName);
    }

    public static String tableFlushTimeRtName(String name) {
        String metricName = String.format("%s%s", name, MetricConstants.UNIT_MS);
        return MetricRegistry.name(MetricConstants.METRIC_TABLE_FLUSH_TIME_RT, metricName);
    }

    public static String tableParserTimeRtName(String name) {
        String metricName = String.format("%s%s", name, MetricConstants.UNIT_US);
        return MetricRegistry.name(MetricConstants.METRIC_TABLE_PARSER_TIME_RT, metricName);
    }

    public static String stepProcessTimeRtName(String name) {
        String metricName = String.format("%s%s", name, MetricConstants.UNIT_US);
        return MetricRegistry.name(MetricConstants.METRIC_STEP_PROCESS_TIME_RT, metricName);
    }

    public static String loadEdgeCountRtName(String name) {
        String metricName = String.format("%s%s", name, MetricConstants.UNIT_N);
        return MetricRegistry.name(MetricConstants.METRIC_LOAD_EDGE_COUNT_RT, metricName);
    }

    public static String loadEdgeTimeRtName(String name) {
        String metricName = String.format("%s%s", name, MetricConstants.UNIT_MS);
        return MetricRegistry.name(MetricConstants.METRIC_LOAD_EDGE_TIME_RT, metricName);
    }

    public static String loadVertexTimeRtName(String name) {
        String metricName = String.format("%s%s", name, MetricConstants.UNIT_MS);
        return MetricRegistry.name(MetricConstants.METRIC_LOAD_VERTEX_TIME_RT, metricName);
    }

}
