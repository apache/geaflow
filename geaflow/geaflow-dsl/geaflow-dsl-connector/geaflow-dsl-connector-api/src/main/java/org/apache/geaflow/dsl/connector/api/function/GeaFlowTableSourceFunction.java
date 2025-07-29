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

package org.apache.geaflow.dsl.connector.api.function;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.geaflow.api.context.RuntimeContext;
import org.apache.geaflow.api.function.RichFunction;
import org.apache.geaflow.api.function.io.SourceFunction;
import org.apache.geaflow.api.window.IWindow;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.common.config.keys.ConnectorConfigKeys;
import org.apache.geaflow.dsl.common.data.Row;
import org.apache.geaflow.dsl.common.exception.GeaFlowDSLException;
import org.apache.geaflow.dsl.common.types.StructType;
import org.apache.geaflow.dsl.connector.api.FetchData;
import org.apache.geaflow.dsl.connector.api.Offset;
import org.apache.geaflow.dsl.connector.api.Partition;
import org.apache.geaflow.dsl.connector.api.TableSource;
import org.apache.geaflow.dsl.connector.api.serde.TableDeserializer;
import org.apache.geaflow.dsl.connector.api.window.FetchWindow;
import org.apache.geaflow.dsl.connector.api.window.FetchWindowFactory;
import org.apache.geaflow.dsl.planner.GQLJavaTypeFactory;
import org.apache.geaflow.dsl.schema.GeaFlowTable;
import org.apache.geaflow.dsl.util.SqlTypeUtil;
import org.apache.geaflow.metrics.common.MetricConstants;
import org.apache.geaflow.metrics.common.MetricGroupRegistry;
import org.apache.geaflow.metrics.common.MetricNameFormatter;
import org.apache.geaflow.metrics.common.api.Counter;
import org.apache.geaflow.metrics.common.api.Histogram;
import org.apache.geaflow.metrics.common.api.Meter;
import org.apache.geaflow.utils.keygroup.KeyGroup;
import org.apache.geaflow.utils.keygroup.KeyGroupAssignment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The implementation of {@link SourceFunction} for DSL table source.
 */
public class GeaFlowTableSourceFunction extends RichFunction implements SourceFunction<Row> {

    private static final Logger LOGGER = LoggerFactory.getLogger(GeaFlowTableSourceFunction.class);

    private static final int PARTITION_COMPARE_PERIOD_SECOND = 120;

    private final GeaFlowTable table;
    private final TableSource tableSource;
    private RuntimeContext runtimeContext;
    private List<Partition> partitions;
    private int parallelism;

    private OffsetStore offsetStore;

    private TableDeserializer<?> deserializer;

    private transient volatile List<Partition> oldPartitions = null;

    private transient volatile boolean isPartitionModified = false;

    private transient volatile boolean isStopPartitionCheck = false;

    private transient ExecutorService singleThreadPool;

    private boolean enableUploadMetrics;
    private Counter rowCounter;
    private Meter rowTps;
    private Meter blockTps;
    private Histogram parserRt;

    public GeaFlowTableSourceFunction(GeaFlowTable table, TableSource tableSource) {
        this.table = table;
        this.tableSource = tableSource;
    }

    @Override
    public void open(RuntimeContext runtimeContext) {
        this.runtimeContext = runtimeContext;
        this.offsetStore = new OffsetStore(runtimeContext, table.getName());
        rowCounter = MetricGroupRegistry.getInstance().getMetricGroup(MetricConstants.MODULE_DSL)
            .counter(MetricNameFormatter.tableInputRowName(table.getName()));
        rowTps = MetricGroupRegistry.getInstance().getMetricGroup(MetricConstants.MODULE_DSL)
            .meter(MetricNameFormatter.tableInputRowTpsName(table.getName()));
        blockTps = MetricGroupRegistry.getInstance().getMetricGroup(MetricConstants.MODULE_DSL)
            .meter(MetricNameFormatter.tableInputBlockTpsName(table.getName()));
        parserRt = MetricGroupRegistry.getInstance().getMetricGroup(MetricConstants.MODULE_DSL)
            .histogram(MetricNameFormatter.tableParserTimeRtName(table.getName()));
    }

    @Override
    public void close() {
        if (tableSource != null) {
            tableSource.close();
        }
        if (singleThreadPool != null) {
            singleThreadPool.shutdownNow();
        }
    }

    @Override
    public void init(int parallel, int index) {
        final long startTime = System.currentTimeMillis();
        this.parallelism = parallel;
        tableSource.open(runtimeContext);
        List<Partition> allPartitions = tableSource.listPartitions(this.parallelism);
        oldPartitions = new ArrayList<>(allPartitions);
        singleThreadPool = startPartitionCompareThread();
        boolean isSingleFileModeRead = table.getConfigWithGlobal(runtimeContext.getConfiguration())
            .getBoolean(ConnectorConfigKeys.GEAFLOW_DSL_SOURCE_FILE_PARALLEL_MOD);

        if (isSingleFileModeRead) {
            Preconditions.checkState(allPartitions.size() == 1,
                "geaflow.dsl.file.single.mod.read is ture only support single file");
            partitions = allPartitions;
        } else {
            partitions = assignPartition(allPartitions,
                runtimeContext.getTaskArgs().getMaxParallelism(), parallel, index);
        }
        for (Partition partition : partitions) {
            partition.setIndex(index, parallel);
        }

        Configuration conf = table.getConfigWithGlobal(runtimeContext.getConfiguration());
        deserializer = tableSource.getDeserializer(conf);
        if (deserializer != null) {
            StructType schema = (StructType) SqlTypeUtil.convertType(
                table.getRowType(GQLJavaTypeFactory.create()));
            deserializer.init(conf, schema);
        }
        enableUploadMetrics = conf.getBoolean(ConnectorConfigKeys.GEAFLOW_DSL_SOURCE_ENABLE_UPLOAD_METRICS);
        LOGGER.info("open source table: {}, taskIndex:{}, parallel: {}, assigned "
            + "partitions:{}, cost {}", table.getName(), index, parallel, partitions, System.currentTimeMillis() - startTime);
    }

    @SuppressWarnings("unchecked")
    @Override
    public boolean fetch(IWindow<Row> window, SourceContext<Row> ctx) throws Exception {
        if (isPartitionModified) {
            throw new GeaFlowDSLException("The partitions of the source table has modified!");
        }
        if (partitions.isEmpty()) {
            return false;
        }
        FetchWindow fetchWindow = FetchWindowFactory.createFetchWindow(window);
        long batchId = window.windowId();
        boolean isFinish = true;
        for (Partition partition : partitions) {
            long partitionStartTime = System.currentTimeMillis();
            Offset offset = offsetStore.readOffset(partition.getName(), batchId);
            FetchData<Object> fetchData = tableSource.fetch(partition, Optional.ofNullable(offset), fetchWindow);
            Iterator<Object> dataIterator = fetchData.getDataIterator();
            while (dataIterator.hasNext()) {
                Object record = dataIterator.next();
                long startTime = System.nanoTime();
                List<Row> rows;
                if (deserializer != null) {
                    rows = ((TableDeserializer<Object>) deserializer).deserialize(record);
                } else {
                    rows = Collections.singletonList((Row) record);
                }
                if (rows != null && rows.size() > 0) {
                    for (Row row : rows) {
                        ctx.collect(row);
                    }
                    if (enableUploadMetrics) {
                        parserRt.update((System.nanoTime() - startTime) / 1000L);
                        rowCounter.inc(rows.size());
                        rowTps.mark(rows.size());
                        blockTps.mark();
                    }
                }
            }
            // store the next offset.
            offsetStore.writeOffset(partition.getName(), batchId + 1, fetchData.getNextOffset());

            LOGGER.info("fetch data size: {}, isFinish: {}, table: {}, partition: {}, batchId: {},"
                    + "nextOffset: {}, cost {}",
                fetchData.getDataSize(), fetchData.isFinish(), table.getName(), partition.getName(),
                batchId, fetchData.getNextOffset().humanReadable(), System.currentTimeMillis() - partitionStartTime);

            if (!fetchData.isFinish()) {
                isFinish = false;
            }
        }
        return !isFinish;
    }

    protected ExecutorService startPartitionCompareThread() {
        ThreadFactory namedThreadFactory = new ThreadFactoryBuilder().setNameFormat(
            "partitionComparedThread" + "-%d").build();

        ExecutorService singleThreadPool = new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS,
            new LinkedBlockingQueue<>(1), namedThreadFactory);
        singleThreadPool.execute(() -> {
            while (!isStopPartitionCheck) {
                if (isPartitionModified) {
                    throw new GeaFlowDSLException(
                        "The partitions of the source table has modified!");
                }
                LOGGER.info("partitionCompareThread is running");
                List<Partition> newPartitions = tableSource.listPartitions(parallelism);
                if (oldPartitions == null || newPartitions == null
                    || oldPartitions.size() != newPartitions.size() || !oldPartitions.equals(
                    newPartitions)) {
                    LOGGER.warn("partition modify. old partition list is: {}, new partition list "
                        + "is: {}", oldPartitions, newPartitions);
                    isPartitionModified = true;
                }
                oldPartitions = newPartitions;
                try {
                    Thread.sleep(PARTITION_COMPARE_PERIOD_SECOND * 1000);
                } catch (InterruptedException e) {
                    isStopPartitionCheck = true;
                }
            }
        });
        return singleThreadPool;
    }

    private List<Partition> assignPartition(List<Partition> allPartitions, int maxParallelism,
                                            int parallel, int index) {
        KeyGroup keyGroup = KeyGroupAssignment.computeKeyGroupRangeForOperatorIndex(maxParallelism,
            parallel, index);

        List<Partition> partitions = new ArrayList<>();
        for (Partition partition : allPartitions) {
            int keyGroupId = KeyGroupAssignment.assignToKeyGroup(partition.getName(),
                maxParallelism);
            if (keyGroupId >= keyGroup.getStartKeyGroup()
                && keyGroupId <= keyGroup.getEndKeyGroup()) {
                partitions.add(partition);
            }
        }
        return partitions;
    }
}
