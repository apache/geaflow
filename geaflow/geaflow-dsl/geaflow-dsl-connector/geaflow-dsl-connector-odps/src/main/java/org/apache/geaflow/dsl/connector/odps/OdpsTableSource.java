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

package org.apache.geaflow.dsl.connector.odps;

import com.aliyun.odps.Odps;
import com.aliyun.odps.PartitionSpec;
import com.aliyun.odps.Table;
import com.aliyun.odps.account.Account;
import com.aliyun.odps.account.AliyunAccount;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.data.RecordReader;
import com.aliyun.odps.tunnel.TableTunnel;
import com.aliyun.odps.tunnel.TableTunnel.DownloadSession;
import com.aliyun.odps.tunnel.TunnelException;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.geaflow.api.context.RuntimeContext;
import org.apache.geaflow.api.window.WindowType;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.dsl.common.data.Row;
import org.apache.geaflow.dsl.common.data.impl.ObjectRow;
import org.apache.geaflow.dsl.common.exception.GeaFlowDSLException;
import org.apache.geaflow.dsl.common.pushdown.EnablePartitionPushDown;
import org.apache.geaflow.dsl.common.pushdown.PartitionFilter;
import org.apache.geaflow.dsl.common.types.StructType;
import org.apache.geaflow.dsl.common.types.TableField;
import org.apache.geaflow.dsl.common.types.TableSchema;
import org.apache.geaflow.dsl.common.util.TypeCastUtil;
import org.apache.geaflow.dsl.connector.api.FetchData;
import org.apache.geaflow.dsl.connector.api.Offset;
import org.apache.geaflow.dsl.connector.api.Partition;
import org.apache.geaflow.dsl.connector.api.TableSource;
import org.apache.geaflow.dsl.connector.api.serde.TableDeserializer;
import org.apache.geaflow.dsl.connector.api.window.FetchWindow;
import org.apache.geaflow.dsl.connector.odps.utils.OdpsBatchIterator;
import org.apache.geaflow.dsl.connector.odps.utils.OdpsConnectorUtils;
import org.apache.geaflow.dsl.connector.odps.utils.OdpsRecordDeserializer;
import org.apache.geaflow.dsl.connector.odps.utils.OdpsRecordWithPartitionSpec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OdpsTableSource implements TableSource, EnablePartitionPushDown {

    private static final Logger LOGGER = LoggerFactory.getLogger(OdpsTableSource.class);

    private int timeoutSeconds = 60;
    private String endPoint;
    private String project;
    private String tableName;
    private String accessKey;
    private String accessId;

    private String shardNamePrefix;
    private PartitionFilter partitionFilter;
    private StructType partitionSchema;
    private StructType schema;
    private Map<String, Integer> columnName2Index;

    private transient Odps odps;
    private transient Table table;
    private transient TableTunnel tunnel;
    private transient Map<OdpsShardPartition, DownloadSession> partition2DownloadSession;

    @Override
    public void init(Configuration tableConf, TableSchema tableSchema) {
        this.endPoint = tableConf.getString(OdpsConfigKeys.GEAFLOW_DSL_ODPS_ENDPOINT);
        this.project = tableConf.getString(OdpsConfigKeys.GEAFLOW_DSL_ODPS_PROJECT);
        this.tableName = tableConf.getString(OdpsConfigKeys.GEAFLOW_DSL_ODPS_TABLE);
        this.accessKey = tableConf.getString(OdpsConfigKeys.GEAFLOW_DSL_ODPS_ACCESS_KEY);
        this.accessId = tableConf.getString(OdpsConfigKeys.GEAFLOW_DSL_ODPS_ACCESS_ID);
        this.shardNamePrefix = project + "-" + tableName + "-";
        this.timeoutSeconds = tableConf.getInteger(OdpsConfigKeys.GEAFLOW_DSL_ODPS_TIMEOUT_SECONDS);

        this.partitionSchema = tableSchema.getPartitionSchema();
        this.schema = Objects.requireNonNull(tableSchema);
        columnName2Index = new HashMap<>();
        for (int i = 0; i < schema.size(); i++) {
            columnName2Index.put(schema.getField(i).getName(), i);
        }

        checkArguments();

        LOGGER.info("init with config: {}, \n schema: {}\n"
                + "endPoint : {}, project : {}, tableName : {}",
            tableConf, tableSchema, endPoint, project, tableName);
    }

    @Override
    public void open(RuntimeContext context) {
        Account account = new AliyunAccount(accessId, accessKey);
        this.odps = new Odps(account);
        odps.setEndpoint(endPoint);
        odps.setDefaultProject(project);
        this.tunnel = new TableTunnel(odps);
        this.partition2DownloadSession = new HashMap<>();
        this.table = odps.tables().get(project, tableName);
        for (TableField field : partitionSchema.getFields()) {
            String partitionKey = field.getName();
            if (this.table.getSchema().getPartitionColumn(partitionKey) == null) {
                throw new GeaFlowDSLException("Partition key: {} not exists in odps table: {}.",
                    partitionKey, tableName);
            }
            if (!OdpsConnectorUtils.typeEquals(
                this.table.getSchema().getPartitionColumn(partitionKey).getTypeInfo().getOdpsType(),
                field.getType())) {
                throw new GeaFlowDSLException("Partition key: {} is {} in Odps but not {}.",
                    partitionKey,
                    this.table.getSchema().getPartitionColumn(partitionKey).getTypeInfo().getTypeName(),
                    field.getType().getName());
            }
        }
        for (TableField field : ((TableSchema) schema).getDataSchema().getFields()) {
            String fieldName = field.getName();
            if (this.table.getSchema().getColumn(fieldName) != null && !OdpsConnectorUtils.typeEquals(
                this.table.getSchema().getColumn(fieldName).getTypeInfo().getOdpsType(),
                field.getType())) {
                throw new GeaFlowDSLException("Column: {} is {} in Odps but not {}.",
                    fieldName,
                    this.table.getSchema().getPartitionColumn(fieldName).getTypeInfo().getTypeName(),
                    field.getType().getName());
            }
        }
    }


    private Row getPartitionRow(PartitionSpec spec) {
        Object[] values = new Object[partitionSchema.getFields().size()];
        int i = 0;
        for (TableField field : partitionSchema.getFields()) {
            String fieldName = field.getName();
            if (spec.keys().contains(fieldName)) {
                values[i] = TypeCastUtil.cast(spec.get(fieldName), field.getType());
            } else {
                values[i] = null;
            }
            i++;
        }
        return ObjectRow.create(values);
    }

    @Override
    public List<Partition> listPartitions() {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        Future<List<Partition>> future = executor.submit(() -> {
            List<com.aliyun.odps.Partition> odpsPartitions = new ArrayList<>();
            List<PartitionSpec> partitionSpecs;
            List<PartitionSpec> allPartitions = table.getPartitionSpecs();
            if (partitionFilter == null || partitionSchema == null) {
                partitionSpecs = allPartitions;
            } else {
                partitionSpecs = allPartitions.stream().filter(p ->
                    partitionFilter.apply(getPartitionRow(p))).collect(Collectors.toList());
            }
            odpsPartitions.addAll(partitionSpecs.stream().map(spec -> table.getPartition(spec)).collect(
                Collectors.toList()));
            return odpsPartitions.stream().map(partition -> new OdpsShardPartition(shardNamePrefix,
                partition.getPartitionSpec())).collect(Collectors.toList());
        });
        List<Partition> odpsPartitions;
        try {
            odpsPartitions = future.get(this.timeoutSeconds, TimeUnit.SECONDS);
        } catch (Exception e) {
            throw new GeaFlowDSLException("Cannot list partitions from ODPS, endPoint: " + this.endPoint, e);
        }
        return odpsPartitions;
    }

    @Override
    public List<Partition> listPartitions(int parallelism) {
        return listPartitions();
    }

    @Override
    public <IN> TableDeserializer<IN> getDeserializer(Configuration conf) {
        return (TableDeserializer<IN>) new OdpsRecordDeserializer();
    }

    @Override
    public <T> FetchData<T> fetch(Partition partition, Optional<Offset> startOffset,
                                  FetchWindow windowInfo) throws IOException {
        long desireWindowSize = -1;
        switch (windowInfo.getType()) {
            case ALL_WINDOW:
                desireWindowSize = Long.MAX_VALUE;
                break;
            case SIZE_TUMBLING_WINDOW:
                desireWindowSize = windowInfo.windowSize();
                break;
            default:
                throw new GeaFlowDSLException("Not support window type:{}", windowInfo.getType());
        }
        assert partition instanceof OdpsShardPartition;
        OdpsShardPartition odpsPartition = (OdpsShardPartition) partition;

        //Get the ODPS download session, and if it does not exist in the map, open one.
        long start = startOffset.isPresent() ? (startOffset.get()).getOffset() : 0L;
        DownloadSession downloadSession = partition2DownloadSession.get(odpsPartition);
        if (downloadSession == null) {
            try {
                downloadSession = tunnel.createDownloadSession(project, tableName,
                    odpsPartition.getSinglePartitionSpec());
                partition2DownloadSession.put(odpsPartition, downloadSession);
            } catch (TunnelException e) {
                throw new GeaFlowDSLException("Cannot get Odps session.", e);
            }
        }
        long sessionRecordCount = downloadSession.getRecordCount();
        long remainingCount = sessionRecordCount - start < 0 ? 0 : sessionRecordCount - start;
        long count = Math.min(desireWindowSize, remainingCount);
        RecordReader reader;
        try {
            reader = downloadSession.openRecordReader(start, count);
        } catch (TunnelException e) {
            throw new GeaFlowDSLException("Cannot get Odps session.", e);
        }

        OdpsOffset nextOffset = new OdpsOffset(start + count);
        if (windowInfo.getType() == WindowType.ALL_WINDOW) {
            return (FetchData<T>) FetchData.createBatchFetch(
                new OdpsBatchIterator(reader, count, odpsPartition.getSinglePartitionSpec()), nextOffset);
        } else {
            List<Object> dataList = new ArrayList<>();
            for (int i = 0; i < count; i++) {
                Record record = reader.read();
                OdpsRecordWithPartitionSpec recordWithPartitionSpec = new OdpsRecordWithPartitionSpec(record, odpsPartition.getSinglePartitionSpec());
                dataList.add(recordWithPartitionSpec);
            }
            reader.close();
            boolean isFinish = desireWindowSize >= remainingCount;
            return (FetchData<T>) FetchData.createStreamFetch(dataList, nextOffset, isFinish);
        }
    }


    @Override
    public void close() {
        LOGGER.info("close.");
    }

    private void checkArguments() {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(endPoint), "endPoint is null");
        Preconditions.checkArgument(!Strings.isNullOrEmpty(project), "project is null");
        Preconditions.checkArgument(!Strings.isNullOrEmpty(tableName), "tableName is null");
        Preconditions.checkArgument(!Strings.isNullOrEmpty(endPoint), "accessKey is null");
        Preconditions.checkArgument(!Strings.isNullOrEmpty(project), "accessId is null");
    }

    @Override
    public void setPartitionFilter(PartitionFilter partitionFilter) {
        this.partitionFilter = partitionFilter;
    }

    public static class OdpsShardPartition implements Partition {

        private final String prefix;
        private final PartitionSpec singlePartitionSpec;

        public OdpsShardPartition(String prefix, PartitionSpec singlePartitionSpec) {
            this.prefix = prefix;
            this.singlePartitionSpec = singlePartitionSpec;
        }

        @Override
        public String getName() {
            return prefix + singlePartitionSpec;
        }

        @Override
        public void setIndex(int index, int parallel) {
        }

        @Override
        public int hashCode() {
            return Objects.hash(prefix, singlePartitionSpec);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof OdpsShardPartition)) {
                return false;
            }
            OdpsShardPartition that = (OdpsShardPartition) o;
            return Objects.equals(prefix, that.prefix) && Objects.equals(
                singlePartitionSpec.toString(), that.singlePartitionSpec.toString());
        }

        public PartitionSpec getSinglePartitionSpec() {
            return singlePartitionSpec;
        }
    }

    public static class OdpsOffset implements Offset {

        private final long offset;

        public OdpsOffset(long offset) {
            this.offset = offset;
        }

        @Override
        public String humanReadable() {
            return String.valueOf(offset);
        }

        @Override
        public boolean isTimestamp() {
            return false;
        }

        @Override
        public long getOffset() {
            return offset;
        }
    }

}
