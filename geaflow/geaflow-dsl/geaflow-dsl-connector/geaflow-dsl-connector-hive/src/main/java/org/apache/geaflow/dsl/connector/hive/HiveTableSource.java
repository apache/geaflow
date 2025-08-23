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

package org.apache.geaflow.dsl.connector.hive;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.geaflow.api.context.RuntimeContext;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.dsl.common.data.Row;
import org.apache.geaflow.dsl.common.data.impl.ObjectRow;
import org.apache.geaflow.dsl.common.exception.GeaFlowDSLException;
import org.apache.geaflow.dsl.common.pushdown.EnablePartitionPushDown;
import org.apache.geaflow.dsl.common.pushdown.PartitionFilter;
import org.apache.geaflow.dsl.common.types.StructType;
import org.apache.geaflow.dsl.common.types.TableSchema;
import org.apache.geaflow.dsl.connector.api.FetchData;
import org.apache.geaflow.dsl.connector.api.Offset;
import org.apache.geaflow.dsl.connector.api.Partition;
import org.apache.geaflow.dsl.connector.api.TableSource;
import org.apache.geaflow.dsl.connector.api.serde.DeserializerFactory;
import org.apache.geaflow.dsl.connector.api.serde.TableDeserializer;
import org.apache.geaflow.dsl.connector.api.window.FetchWindow;
import org.apache.geaflow.dsl.connector.hive.adapter.HiveVersionAdapter;
import org.apache.geaflow.dsl.connector.hive.adapter.HiveVersionAdapters;
import org.apache.geaflow.dsl.connector.hive.util.HiveUtils;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HiveTableSource implements TableSource, EnablePartitionPushDown {

    private static final Logger LOGGER = LoggerFactory.getLogger(HiveTableSource.class);

    private StructType dataSchema;
    private TableSchema tableSchema;
    private String dbName;
    private String tableName;
    private String metastoreURIs;
    private int splitNumPerPartition;
    private transient IMetaStoreClient metaClient;
    private transient Map<String, HiveReader> partitionReaders;

    private Table hiveTable;
    private Properties tableProps;

    private PartitionFilter partitionFilter;

    @Override
    public void init(Configuration conf, TableSchema tableSchema) {
        this.dataSchema = tableSchema.getDataSchema();
        this.tableSchema = tableSchema;
        this.dbName = conf.getString(HiveConfigKeys.GEAFLOW_DSL_HIVE_DATABASE_NAME);
        this.tableName = conf.getString(HiveConfigKeys.GEAFLOW_DSL_HIVE_TABLE_NAME);
        this.metastoreURIs = conf.getString(HiveConfigKeys.GEAFLOW_DSL_HIVE_METASTORE_URIS);
        this.splitNumPerPartition = conf.getInteger(HiveConfigKeys.GEAFLOW_DSL_HIVE_PARTITION_MIN_SPLITS);
    }

    @Override
    public void open(RuntimeContext context) {
        this.partitionReaders = new HashMap<>();
        Map<String, String> hiveConf = new HashMap<>();
        hiveConf.put(HiveUtils.THRIFT_URIS, metastoreURIs);
        HiveVersionAdapter hiveVersionAdapter = HiveVersionAdapters.get();
        this.metaClient = hiveVersionAdapter.createMetaSoreClient(HiveUtils.getHiveConfig(hiveConf));
        try {
            this.hiveTable = metaClient.getTable(dbName, tableName);
        } catch (Exception e) {
            throw new GeaFlowDSLException("Fail to get hive table for: {}.{}", dbName, tableName);
        }
        this.tableProps = hiveVersionAdapter.getTableMetadata(hiveTable);
        LOGGER.info("open hive table source, hive version is: {}", hiveVersionAdapter.version());
    }

    @Override
    public List<Partition> listPartitions() {
        List<Partition> allPartitions = new ArrayList<>();
        try {
            List<org.apache.hadoop.hive.metastore.api.Partition> hivePartitions =
                metaClient.listPartitions(dbName, tableName, (short) -1);

            List<StorageDescriptor> storageDescriptors = new ArrayList<>();
            List<String[]> sdPartitionValues = new ArrayList<>();
            if (hivePartitions != null && !hivePartitions.isEmpty()) {
                for (org.apache.hadoop.hive.metastore.api.Partition hivePartition : hivePartitions) {
                    String[] partitionValues = alignPartitionValues(hivePartition.getValues());
                    if (accept(partitionValues)) {
                        storageDescriptors.add(hivePartition.getSd());
                        sdPartitionValues.add(partitionValues);
                    }
                }
            } else {
                storageDescriptors.add(hiveTable.getSd());
                sdPartitionValues.add(new String[0]);
            }
            for (int i = 0; i < storageDescriptors.size(); i++) {
                StorageDescriptor sd = storageDescriptors.get(i);
                String[] partitionValues = sdPartitionValues.get(i);
                InputFormat<Writable, Writable> inputFormat = HiveUtils.createInputFormat(sd);
                InputSplit[] hadoopInputSplits = HiveUtils.createInputSplits(sd, inputFormat, splitNumPerPartition);
                if (hadoopInputSplits != null) {
                    for (InputSplit split : hadoopInputSplits) {
                        allPartitions.add(
                            new HivePartition(dbName, tableName, split, inputFormat, sd, partitionValues));
                    }
                }
            }
        } catch (Exception e) {
            throw new GeaFlowDSLException("fail to list partitions for " + dbName + "." + tableName, e);
        }
        return allPartitions;
    }

    @Override
    public List<Partition> listPartitions(int parallelism) {
        return listPartitions();
    }

    /**
     * Align the hive partition values to the partition fields order defined in DSL ddl.
     */
    private String[] alignPartitionValues(List<String> partitionValues) {
        List<String> hivePartitionKeys = hiveTable.getPartitionKeys().stream()
            .map(FieldSchema::getName)
            .collect(Collectors.toList());
        StructType partitionSchema = tableSchema.getPartitionSchema();

        String[] alignedValues = new String[partitionSchema.size()];
        for (int i = 0; i < partitionSchema.size(); i++) {
            String partitionName = partitionSchema.getField(i).getName();
            int valueIndex = hivePartitionKeys.indexOf(partitionName);
            if (valueIndex >= 0) {
                alignedValues[i] = partitionValues.get(valueIndex);
            } else {
                alignedValues[i] = null;
            }
        }
        return alignedValues;
    }

    private boolean accept(Object[] partitionValues) {
        if (partitionFilter != null) {
            Row row = ObjectRow.create(partitionValues);
            return partitionFilter.apply(row);
        }
        return true;
    }

    @SuppressWarnings("unchecked")
    @Override
    public <IN> TableDeserializer<IN> getDeserializer(Configuration conf) {
        return DeserializerFactory.loadRowTableDeserializer();
    }

    @SuppressWarnings("unchecked")
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
        HivePartition hivePartition = (HivePartition) partition;
        HiveReader reader = partitionReaders.get(partition.getName());
        if (reader == null) {
            JobConf jobConf = HiveUtils.getJobConf(hivePartition.getSd());
            Reporter reporter = HiveUtils.createDummyReporter();
            RecordReader<Writable, Writable> recordReader = hivePartition.getInputFormat()
                .getRecordReader(hivePartition.getSplit(), jobConf, reporter);
            if (recordReader instanceof Configurable) {
                ((Configurable) recordReader).setConf(jobConf);
            }
            reader = new HiveReader(recordReader, dataSchema, hivePartition.getSd(), tableProps);
            partitionReaders.put(partition.getName(), reader);
        }
        try {
            return (FetchData<T>) reader.read(desireWindowSize, hivePartition.getPartitionValues());
        } catch (Exception e) {
            throw new GeaFlowDSLException(e);
        }
    }

    @Override
    public void close() {
        if (metaClient != null) {
            metaClient.close();
        }
        LOGGER.info("close hive table source: {}", tableName);
    }

    @Override
    public void setPartitionFilter(PartitionFilter partitionFilter) {
        this.partitionFilter = partitionFilter;
    }

    public static class HivePartition implements Partition {

        private final String dbName;
        private final String tableName;
        private final InputSplit split;
        private final InputFormat<Writable, Writable> inputFormat;
        private final StorageDescriptor sd;
        private final String[] partitionValues;

        public HivePartition(String dbName,
                             String tableName,
                             InputSplit split,
                             InputFormat<Writable, Writable> inputFormat,
                             StorageDescriptor sd,
                             String[] partitionValues) {
            this.dbName = dbName;
            this.tableName = tableName;
            this.split = split;
            this.inputFormat = inputFormat;
            this.sd = sd;
            this.partitionValues = partitionValues;
        }

        @Override
        public String getName() {
            return StringUtils.join(new Object[]{dbName, tableName, split}, "-");
        }

        @Override
        public void setIndex(int index, int parallel) {
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof HivePartition)) {
                return false;
            }
            HivePartition that = (HivePartition) o;
            return Objects.equals(dbName, that.dbName) && Objects.equals(tableName, that.tableName)
                && Objects.equals(split != null ? split.toString() : "null",
                that.split != null ? that.split.toString() : "null");
        }

        @Override
        public int hashCode() {
            return Objects.hash(dbName, tableName, split != null ? split.toString() : "null");
        }

        public InputSplit getSplit() {
            return split;
        }

        public InputFormat<Writable, Writable> getInputFormat() {
            return inputFormat;
        }

        public StorageDescriptor getSd() {
            return sd;
        }

        public String[] getPartitionValues() {
            return partitionValues;
        }
    }

    public static class HiveOffset implements Offset {

        private final long offset;

        public HiveOffset(long offset) {
            this.offset = offset;
        }

        @Override
        public String humanReadable() {
            return String.valueOf(offset);
        }

        @Override
        public long getOffset() {
            return offset;
        }

        @Override
        public boolean isTimestamp() {
            return false;
        }
    }
}
