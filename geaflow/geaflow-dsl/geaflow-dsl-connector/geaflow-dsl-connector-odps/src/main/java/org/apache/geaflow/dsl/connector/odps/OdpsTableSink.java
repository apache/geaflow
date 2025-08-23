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
import com.aliyun.odps.account.Account;
import com.aliyun.odps.account.AliyunAccount;
import com.aliyun.odps.data.ArrayRecord;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.data.RecordWriter;
import com.aliyun.odps.tunnel.TableTunnel;
import com.aliyun.odps.tunnel.TableTunnel.UploadSession;
import com.aliyun.odps.tunnel.TunnelException;
import com.aliyun.odps.utils.StringUtils;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
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
    private int timeoutSeconds = 60;
    private String endPoint;
    private String project;
    private String tableName;
    private String accessKey;
    private String accessId;
    private String partitionSpec;
    private String shardNamePrefix;
    private StructType schema;

    private transient Odps odps;
    private transient Table table;
    private transient TableTunnel tunnel;
    private transient UploadSession uploadSession;
    private transient RecordWriter writer;
    private transient Column[] recordColumns;
    private transient int[] columnIndex;
    private transient List<Object[]> buffer;

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
        this.shardNamePrefix = project + "-" + tableName + "-";
        int bufferSize = tableConf.getInteger(OdpsConfigKeys.GEAFLOW_DSL_ODPS_SINK_BUFFER_SIZE);
        if (bufferSize > 0) {
            this.bufferSize = bufferSize;
        }
        int timeoutSeconds = tableConf.getInteger(OdpsConfigKeys.GEAFLOW_DSL_ODPS_TIMEOUT_SECONDS);
        if (timeoutSeconds > 0) {
            this.timeoutSeconds = timeoutSeconds;
        }
        checkArguments();

        LOGGER.info("init odps table sink, endPoint : {},  project : {}, tableName : {}, "
            + "partitionSpec: {}", endPoint, project, tableName, partitionSpec);
    }

    @Override
    public void open(RuntimeContext context) {
        this.buffer = new ArrayList<>(bufferSize);
        Account account = new AliyunAccount(accessId, accessKey);
        this.odps = new Odps(account);
        odps.setEndpoint(endPoint);
        odps.setDefaultProject(project);
        this.table = odps.tables().get(project, tableName);
        this.tunnel = new TableTunnel(odps);
        if (StringUtils.isEmpty(partitionSpec)) {
            throw new GeaFlowDSLException("For ODPS sink, partition spec cannot be empty.");
        }
        PartitionSpec usePartitionSpec = new PartitionSpec(partitionSpec);
        ExecutorService executor = Executors.newSingleThreadExecutor();
        Future<UploadSession> future = executor.submit(() -> {
            try {
                return tunnel.createUploadSession(project, tableName, usePartitionSpec);
            } catch (TunnelException e) {
                throw new GeaFlowDSLException("Cannot get odps session.", e);
            }
        });
        try {
            this.uploadSession = future.get(this.timeoutSeconds, TimeUnit.SECONDS);
        } catch (Exception e) {
            throw new GeaFlowDSLException("Cannot list partitions from ODPS, endPoint: " + this.endPoint, e);
        }
        this.recordColumns = this.uploadSession.getSchema().getColumns().toArray(new Column[0]);
        this.columnIndex = new int[recordColumns.length];
        for (int i = 0; i < this.recordColumns.length; i++) {
            String columnName = this.recordColumns[i].getName();
            columnIndex[i] = this.schema.indexOf(columnName);
        }
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
        buffer.add(values);
        if (buffer.size() >= bufferSize) {
            try {
                flush();
            } catch (Exception e) {
                throw new IOException(e);
            }
        }
    }

    @Override
    public void finish() throws IOException {
        try {
            flush();
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    @Override
    public void close() {
        LOGGER.info("close.");
        if (writer != null) {
            try {
                writer.close();
            } catch (IOException e) {
                throw new GeaFlowDSLException("Error when closing Odps writer.", e);
            }
        }
    }

    private void flush() throws IOException {
        if (buffer.isEmpty()) {
            return;
        }
        List<Object[]> flushBuffer = buffer;
        buffer = new ArrayList<>(bufferSize);
        List<Record> records = new ArrayList<>();
        for (Object[] value : flushBuffer) {
            Record record = new ArrayRecord(recordColumns, value);
            records.add(record);
        }
        if (writer == null) {
            try {
                assert uploadSession != null : "The uploadSession has not been open.";
                writer = uploadSession.openBufferedWriter();
            } catch (TunnelException e) {
                throw new GeaFlowDSLException("Cannot get Odps writer.", e);
            }
        }
        for (Record record : records) {
            writer.write(record);
        }
    }

    private void checkArguments() {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(endPoint), "endPoint is null");
        Preconditions.checkArgument(!Strings.isNullOrEmpty(project), "project is null");
        Preconditions.checkArgument(!Strings.isNullOrEmpty(tableName), "tableName is null");
        Preconditions.checkArgument(!Strings.isNullOrEmpty(endPoint), "accessKey is null");
        Preconditions.checkArgument(!Strings.isNullOrEmpty(project), "accessId is null");
        Preconditions.checkNotNull(new PartitionSpec(partitionSpec));
    }
}
