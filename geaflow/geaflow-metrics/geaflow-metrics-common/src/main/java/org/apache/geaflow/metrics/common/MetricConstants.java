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

public class MetricConstants {

    public static final String GROUP_DELIMITER = "/";

    /**
     * Metric module.
     */
    public static final String MODULE_DEFAULT = "default";
    public static final String MODULE_SYSTEM = "system";
    public static final String MODULE_DSL = "dsl";
    public static final String MODULE_FRAMEWORK = "framework";

    /**
     * System metric name.
     */
    public static final String METRIC_TOTAL_HEAP = "totalUsedHeapMb";
    public static final String METRIC_TOTAL_MEMORY = "totalMemoryMb";
    public static final String METRIC_HEAP_USAGE_RATIO = "heapUsageRatio";
    public static final String METRIC_GC_TIME = "gcTime";
    public static final String METRIC_FGC_COUNT = "fgcCount";
    public static final String METRIC_FGC_TIME = "fgcTime";

    /**
     * Operator metric name.
     */
    public static final String METRIC_INPUT_TPS = "inputTps";
    public static final String METRIC_OUTPUT_TPS = "outputTps";
    public static final String METRIC_VERTEX_TPS = "vertexTps";
    public static final String METRIC_EDGE_TPS = "edgeTps";
    public static final String METRIC_PROCESS_RT = "processRt";
    public static final String METRIC_ITERATION = "iteration";
    public static final String METRIC_ITERATION_MSG_TPS = "iterationMsgTps";
    public static final String METRIC_ITERATION_AGG_TPS = "iterationAggTps";

    /**
     * Dsl metric name.
     */
    public static final String METRIC_TABLE_INPUT_ROW = "tableInputRow";
    public static final String METRIC_STEP_INPUT_RECORD = "stepInputRecord";
    public static final String METRIC_STEP_OUTPUT_RECORD = "stepOutputRecord";
    public static final String METRIC_STEP_INPUT_EOD = "stepInputEod";
    public static final String METRIC_TABLE_OUTPUT_ROW_TPS = "tableOutputRowTps";
    public static final String METRIC_TABLE_INPUT_ROW_TPS = "tableInputRowTps";
    public static final String METRIC_TABLE_INPUT_BLOCK_TPS = "tableInputBlockTps";
    public static final String METRIC_STEP_INPUT_ROW_TPS = "stepInputRowTps";
    public static final String METRIC_STEP_OUTPUT_ROW_TPS = "stepOutputRowTps";
    public static final String METRIC_TABLE_WRITE_TIME_RT = "tableWriteTimeRt";
    public static final String METRIC_TABLE_FLUSH_TIME_RT = "tableFlushTimeRt";
    public static final String METRIC_TABLE_PARSER_TIME_RT = "tableParserTimeRt";
    public static final String METRIC_STEP_PROCESS_TIME_RT = "stepProcessTimeRt";
    public static final String METRIC_LOAD_EDGE_COUNT_RT = "loadEdgeCountRt";
    public static final String METRIC_LOAD_EDGE_TIME_RT = "loadEdgeTimeRt";
    public static final String METRIC_LOAD_VERTEX_TIME_RT = "loadVertexTimeRt";

    /**
     * Metric unit.
     */
    public static final String UNIT_N = "(N)";
    public static final String UNIT_S = "(s)";
    public static final String UNIT_MS = "(ms)";
    public static final String UNIT_US = "(us)";
    public static final String UNIT_NS = "(ns)";
    public static final String UNIT_ROW_PER_S = "(row/s)";
    public static final String UNIT_BLOCK_PER_S = "(block/s)";

}
