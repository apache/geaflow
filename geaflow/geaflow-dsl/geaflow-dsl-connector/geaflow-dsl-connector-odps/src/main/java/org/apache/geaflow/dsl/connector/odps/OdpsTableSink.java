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

import com.aliyun.odps.Column;
import com.aliyun.odps.Odps;
import com.aliyun.odps.PartitionSpec;
import com.aliyun.odps.Table;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.account.Account;
import com.aliyun.odps.account.AliyunAccount;
import com.aliyun.odps.data.ArrayRecord;
import com.aliyun.odps.tunnel.TableTunnel;
import com.aliyun.odps.tunnel.TunnelException;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import org.apache.geaflow.api.context.RuntimeContext;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.dsl.common.data.Row;
import org.apache.geaflow.dsl.common.exception.GeaFlowDSLException;
import org.apache.geaflow.dsl.common.types.StructType;
import org.apache.geaflow.dsl.connector.api.TableSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OdpsTableSink implements TableSink {

    private static final Logger LOGGER = LoggerFactory.getLogger(OdpsTableSink.class);

    private int bufferSize = 1000;
    private int flushIntervalMs = Integer.MAX_VALUE;

    private int timeoutSeconds = 60;
    private String endPoint;
    private String project;
    private String tableName;
    private String accessKey;
    private String accessId;
    private StructType schema;
    private String partitionSpec;

    private transient TableTunnel tunnel;
    private transient Column[] recordColumns;
    private transient int[] columnIndex;
    private transient PartitionExtractor partitionExtractor;
    private transient Map<String, PartitionWriter> partitionWriters;

    private final ExecutorService executor = Executors.newSingleThreadExecutor();

    @Override
    public void init(Configuration tableConf, StructType schema) {
        LOGGER.info("open with config: {}, \n schema: {}", tableConf, schema);
        this.schema = Objects.requireNonNull(schema);
        this.columnIndex = new int[schema.size()];
        for (int i = 0; i < this.schema.size(); i++) {
            String columnName = this.schema.getField(i).getName();
            columnIndex[i] = this.schema.indexOf(columnName);
        }
        this.endPoint = tableConf.getString(OdpsConfigKeys.GEAFLOW_DSL_ODPS_ENDPOINT);
        this.project = tableConf.getString(OdpsConfigKeys.GEAFLOW_DSL_ODPS_PROJECT);
        this.tableName = tableConf.getString(OdpsConfigKeys.GEAFLOW_DSL_ODPS_TABLE);
        this.accessKey = tableConf.getString(OdpsConfigKeys.GEAFLOW_DSL_ODPS_ACCESS_KEY);
        this.accessId = tableConf.getString(OdpsConfigKeys.GEAFLOW_DSL_ODPS_ACCESS_ID);
        this.partitionSpec = tableConf.getString(OdpsConfigKeys.GEAFLOW_DSL_ODPS_PARTITION_SPEC);
        int bufferSize = tableConf.getInteger(OdpsConfigKeys.GEAFLOW_DSL_ODPS_SINK_BUFFER_SIZE);
        if (bufferSize > 0) {
            this.bufferSize = bufferSize;
        }
        int flushIntervalMs = tableConf.getInteger(OdpsConfigKeys.GEAFLOW_DSL_ODPS_SINK_FLUSH_INTERVAL_MS);
        if (flushIntervalMs > 0) {
            this.flushIntervalMs = flushIntervalMs;
        }
        int timeoutSeconds = tableConf.getInteger(OdpsConfigKeys.GEAFLOW_DSL_ODPS_TIMEOUT_SECONDS);
        if (timeoutSeconds > 0) {
            this.timeoutSeconds = timeoutSeconds;
        }
        checkArguments();

        LOGGER.info("init odps table sink, endPoint : {},  project : {}, tableName : {}",
                endPoint, project, tableName);
    }

    @Override
    public void open(RuntimeContext context) {
        Account account = new AliyunAccount(accessId, accessKey);
        Odps odps = new Odps(account);
        odps.setEndpoint(endPoint);
        odps.setDefaultProject(project);
        this.tunnel = new TableTunnel(odps);
        Table table = odps.tables().get(tableName);
        TableSchema tableSchema = table.getSchema();
        this.recordColumns = tableSchema.getColumns().toArray(new Column[0]);
        this.columnIndex = new int[recordColumns.length];
        for (int i = 0; i < this.recordColumns.length; i++) {
            String columnName = this.recordColumns[i].getName();
            columnIndex[i] = this.schema.indexOf(columnName);
        }
        if (this.partitionSpec != null && !this.partitionSpec.isEmpty()) {
            this.partitionExtractor = DefaultPartitionExtractor.create(this.partitionSpec, schema);
        } else {
            List<Column> partitionColumns = tableSchema.getPartitionColumns();
            this.partitionExtractor = DefaultPartitionExtractor.create(partitionColumns, schema);
        }
        this.partitionWriters = new HashMap<>();
    }

    @Override
    public void write(Row row) throws IOException {
        Object[] values = new Object[columnIndex.length];
        for (int i = 0; i < columnIndex.length; i++) {
            if (columnIndex[i] >= 0) {
                values[i] = row.getField(columnIndex[i], schema.getType(columnIndex[i]));
            } else {
                values[i] = null;
            }
        }
        PartitionWriter writer = createOrGetWriter(partitionExtractor.extractPartition(row));
        writer.write(new ArrayRecord(recordColumns, values));
    }

    @Override
    public void finish() throws IOException {
        flush();
    }

    @Override
    public void close() {
        LOGGER.info("close.");
        flush();
    }

    private void flush() {
        try {
            for (PartitionWriter writer : partitionWriters.values()) {
                writer.flush();
            }
        } catch (IOException e) {
            throw new RuntimeException("Flush data error.", e);
        }
    }

    /**
     * Create or get writer.
     * @param partition the partition
     * @return a writer
     */
    private PartitionWriter createOrGetWriter(String partition) {
        PartitionWriter partitionWriter = partitionWriters.get(partition);
        if (partitionWriter == null) {
            TableTunnel.StreamUploadSession session = createUploadSession(partition);
            partitionWriter = new PartitionWriter(session, bufferSize, flushIntervalMs);
            partitionWriters.put(partition, partitionWriter);
        }
        return partitionWriter;
    }

    /**
     * Create an upload session.
     * @return an upload session
     */
    private TableTunnel.StreamUploadSession createUploadSession() {
        return createUploadSession(null);
    }

    /**
     * Create an upload session.
     * @param partition the partition
     * @return an upload session
     */
    private TableTunnel.StreamUploadSession createUploadSession(@Nullable String partition) {
        Future<TableTunnel.StreamUploadSession> future = executor.submit(() -> {
            try {
                if (partition == null || partition.isEmpty()) {
                    return tunnel.createStreamUploadSession(project, tableName);
                }
                return tunnel.createStreamUploadSession(project, tableName, new PartitionSpec(partition));
            } catch (TunnelException e) {
                throw new GeaFlowDSLException("Cannot get odps session.", e);
            }
        });
        try {
            return future.get(this.timeoutSeconds, TimeUnit.SECONDS);
        } catch (Exception e) {
            throw new GeaFlowDSLException("Create stream upload session with endpoint " + this.endPoint + " failed", e);
        }
    }

    private void checkArguments() {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(endPoint), "endPoint is null");
        Preconditions.checkArgument(!Strings.isNullOrEmpty(project), "project is null");
        Preconditions.checkArgument(!Strings.isNullOrEmpty(tableName), "tableName is null");
        Preconditions.checkArgument(!Strings.isNullOrEmpty(accessId), "accessId is null");
        Preconditions.checkArgument(!Strings.isNullOrEmpty(accessKey), "accessKey is null");
    }
}
