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

package org.apache.geaflow.dsl.connector.clickhouse;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import org.apache.geaflow.api.context.RuntimeContext;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.dsl.common.data.Row;
import org.apache.geaflow.dsl.common.exception.GeaFlowDSLException;
import org.apache.geaflow.dsl.common.types.StructType;
import org.apache.geaflow.dsl.common.types.TableField;
import org.apache.geaflow.dsl.connector.api.TableSink;
import org.apache.geaflow.dsl.connector.clickhouse.util.ClickHouseUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ClickHouse table sink. Unlike a row-by-row JDBC INSERT, rows are buffered onto a reusable
 * prepared statement and flushed in bulk once the configured batch size is reached (and again when
 * the window finishes). ClickHouse is columnar and ingests far more efficiently from bulk inserts,
 * so this is the main performance win over connecting through the generic JDBC connector.
 */
public class ClickHouseTableSink implements TableSink {

    private static final Logger LOGGER = LoggerFactory.getLogger(ClickHouseTableSink.class);

    private StructType schema;
    private String driver;
    private String url;
    private String username;
    private String password;
    private String tableName;
    private int batchSize;

    private transient Connection connection;
    private transient PreparedStatement statement;
    private transient int bufferedRows;

    @Override
    public void init(Configuration tableConf, StructType tableSchema) {
        LOGGER.info("init clickhouse sink with config: {}, schema: {}", tableConf, tableSchema);
        this.schema = tableSchema;
        this.driver = tableConf.getString(ClickHouseConfigKeys.GEAFLOW_DSL_CLICKHOUSE_DRIVER);
        this.url = tableConf.getString(ClickHouseConfigKeys.GEAFLOW_DSL_CLICKHOUSE_URL);
        this.username = tableConf.getString(ClickHouseConfigKeys.GEAFLOW_DSL_CLICKHOUSE_USERNAME);
        this.password = tableConf.getString(ClickHouseConfigKeys.GEAFLOW_DSL_CLICKHOUSE_PASSWORD);
        this.tableName = tableConf.getString(ClickHouseConfigKeys.GEAFLOW_DSL_CLICKHOUSE_TABLE_NAME);
        this.batchSize =
            tableConf.getInteger(ClickHouseConfigKeys.GEAFLOW_DSL_CLICKHOUSE_WRITE_BATCH_SIZE);
        if (this.batchSize <= 0) {
            throw new GeaFlowDSLException("clickhouse write batch size must be > 0, but was {}",
                this.batchSize);
        }
    }

    @Override
    public void open(RuntimeContext context) {
        try {
            Class.forName(this.driver);
            this.connection = DriverManager.getConnection(url, username, password);
            List<TableField> fields = schema.getFields();
            String insertSql = ClickHouseUtils.buildInsertSql(tableName, fields);
            this.statement = connection.prepareStatement(insertSql);
            this.bufferedRows = 0;
            LOGGER.info("open clickhouse sink for table {} with batch size {}", tableName, batchSize);
        } catch (ClassNotFoundException e) {
            throw new GeaFlowDSLException("failed to load clickhouse driver: " + driver, e);
        } catch (SQLException e) {
            throw new GeaFlowDSLException("failed to connect to clickhouse: " + url, e);
        }
    }

    @Override
    public void write(Row row) throws IOException {
        try {
            ClickHouseUtils.bindRow(statement, schema.getFields(), row);
            statement.addBatch();
            bufferedRows++;
            if (bufferedRows >= batchSize) {
                flush();
            }
        } catch (SQLException e) {
            throw new GeaFlowDSLException("failed to buffer row for table: " + tableName, e);
        }
    }

    @Override
    public void finish() throws IOException {
        try {
            flush();
        } catch (SQLException e) {
            throw new GeaFlowDSLException("failed to flush batch to table: " + tableName, e);
        }
    }

    private void flush() throws SQLException {
        if (bufferedRows == 0) {
            return;
        }
        try {
            statement.executeBatch();
            LOGGER.info("flushed {} rows to clickhouse table {}", bufferedRows, tableName);
        } finally {
            // Always drop the buffered batch, even if executeBatch failed, so a failed flush is
            // not silently re-sent (and re-counted) on the next flush or on close.
            bufferedRows = 0;
            try {
                statement.clearBatch();
            } catch (SQLException e) {
                LOGGER.warn("failed to clear batch after flush", e);
            }
        }
    }

    @Override
    public void close() {
        SQLException error = null;
        if (this.statement != null) {
            try {
                this.statement.close();
            } catch (SQLException e) {
                error = e;
            } finally {
                this.statement = null;
            }
        }
        // Close the connection even if closing the statement failed, so a statement-close error
        // cannot leak the connection.
        if (this.connection != null) {
            try {
                this.connection.close();
            } catch (SQLException e) {
                if (error == null) {
                    error = e;
                }
            } finally {
                this.connection = null;
            }
        }
        LOGGER.info("close clickhouse sink");
        if (error != null) {
            throw new GeaFlowDSLException("failed to close clickhouse sink", error);
        }
    }
}
