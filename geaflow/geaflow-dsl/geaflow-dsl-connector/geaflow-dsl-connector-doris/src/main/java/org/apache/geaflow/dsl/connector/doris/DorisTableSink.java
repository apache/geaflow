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

package org.apache.geaflow.dsl.connector.doris;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import org.apache.geaflow.api.context.RuntimeContext;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.dsl.common.data.Row;
import org.apache.geaflow.dsl.common.exception.GeaFlowDSLException;
import org.apache.geaflow.dsl.common.types.StructType;
import org.apache.geaflow.dsl.common.types.TableField;
import org.apache.geaflow.dsl.connector.api.TableSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A Doris table sink that buffers rows and flushes them to Doris via Stream Load. Rows are
 * accumulated in {@link #write(Row)} and a whole batch is loaded once the buffered row count or
 * byte size reaches the configured threshold (or when {@link #finish()} is called at the end of a
 * window). This batched load path gives a much higher write throughput than row-by-row JDBC
 * INSERT.
 */
public class DorisTableSink implements TableSink {

    private static final Logger LOGGER = LoggerFactory.getLogger(DorisTableSink.class);

    private StructType schema;
    private String feNode;
    private String database;
    private String table;
    private String username;
    private String password;
    private String format;
    private String columnSeparator;
    private String lineDelimiter;
    private long maxRows;
    private long maxBytes;
    private int maxRetries;
    private int connectTimeoutMs;
    private int readTimeoutMs;

    private transient DorisStreamLoad streamLoad;
    private transient List<String> buffer;
    private transient long bufferBytes;

    @Override
    public void init(Configuration conf, StructType tableSchema) {
        LOGGER.info("init doris sink with config: {}, \n schema: {}", conf, tableSchema);
        this.schema = tableSchema;
        String feNodes = conf.getString(DorisConfigKeys.GEAFLOW_DSL_DORIS_FENODES, "");
        if (feNodes == null || feNodes.trim().isEmpty()) {
            throw new GeaFlowDSLException("Doris fenodes must be specified for the sink.");
        }
        this.feNode = feNodes.split(DorisConstants.COMMA)[0].trim();
        this.database = conf.getString(DorisConfigKeys.GEAFLOW_DSL_DORIS_DATABASE);
        this.table = conf.getString(DorisConfigKeys.GEAFLOW_DSL_DORIS_TABLE);
        this.username = conf.getString(DorisConfigKeys.GEAFLOW_DSL_DORIS_USERNAME);
        this.password = conf.getString(DorisConfigKeys.GEAFLOW_DSL_DORIS_PASSWORD);
        this.format = conf.getString(DorisConfigKeys.GEAFLOW_DSL_DORIS_SINK_FORMAT);
        this.columnSeparator = conf.getString(DorisConfigKeys.GEAFLOW_DSL_DORIS_SINK_COLUMN_SEPARATOR);
        this.lineDelimiter = conf.getString(DorisConfigKeys.GEAFLOW_DSL_DORIS_SINK_LINE_DELIMITER);
        this.maxRows = conf.getLong(DorisConfigKeys.GEAFLOW_DSL_DORIS_SINK_MAX_ROWS);
        this.maxBytes = conf.getLong(DorisConfigKeys.GEAFLOW_DSL_DORIS_SINK_MAX_BYTES);
        this.maxRetries = conf.getInteger(DorisConfigKeys.GEAFLOW_DSL_DORIS_SINK_MAX_RETRIES);
        this.connectTimeoutMs = conf.getInteger(DorisConfigKeys.GEAFLOW_DSL_DORIS_REQUEST_CONNECT_TIMEOUT_MS);
        this.readTimeoutMs = conf.getInteger(DorisConfigKeys.GEAFLOW_DSL_DORIS_REQUEST_READ_TIMEOUT_MS);
    }

    @Override
    public void open(RuntimeContext context) {
        List<String> columns = new ArrayList<>();
        for (TableField field : schema.getFields()) {
            columns.add(field.getName());
        }
        this.streamLoad = new DorisStreamLoad(feNode, database, table, username, password, format,
            columnSeparator, lineDelimiter, columns, connectTimeoutMs, readTimeoutMs, maxRetries);
        this.buffer = new ArrayList<>();
        this.bufferBytes = 0L;
    }

    @Override
    public void write(Row row) throws IOException {
        String record = DorisConstants.FORMAT_JSON.equalsIgnoreCase(format)
            ? DorisUtils.rowToJson(row, schema)
            : DorisUtils.rowToCsv(row, schema, columnSeparator);
        buffer.add(record);
        bufferBytes += record.getBytes(StandardCharsets.UTF_8).length;
        if (buffer.size() >= maxRows || bufferBytes >= maxBytes) {
            flushBuffer();
        }
    }

    @Override
    public void finish() throws IOException {
        flushBuffer();
    }

    private void flushBuffer() {
        if (buffer == null || buffer.isEmpty()) {
            return;
        }
        String payload;
        if (DorisConstants.FORMAT_JSON.equalsIgnoreCase(format)) {
            payload = "[" + String.join(DorisConstants.COMMA, buffer) + "]";
        } else {
            payload = String.join(lineDelimiter, buffer);
        }
        streamLoad.load(payload.getBytes(StandardCharsets.UTF_8));
        LOGGER.info("flushed {} rows ({} bytes) to doris table {}.{}", buffer.size(), bufferBytes,
            database, table);
        buffer.clear();
        bufferBytes = 0L;
    }

    @Override
    public void close() {
        try {
            if (streamLoad != null) {
                streamLoad.close();
                streamLoad = null;
            }
        } catch (IOException e) {
            throw new GeaFlowDSLException("failed to close doris stream load client.", e);
        }
    }
}
