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

package org.apache.geaflow.dsl.connector.jdbc;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import org.apache.geaflow.api.context.RuntimeContext;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.dsl.common.data.Row;
import org.apache.geaflow.dsl.common.exception.GeaFlowDSLException;
import org.apache.geaflow.dsl.common.types.StructType;
import org.apache.geaflow.dsl.connector.api.TableSink;
import org.apache.geaflow.dsl.connector.jdbc.util.JDBCUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JDBCTableSink implements TableSink {

    private static final Logger LOGGER = LoggerFactory.getLogger(JDBCTableSink.class);

    private Configuration tableConf;
    private StructType schema;
    private String driver;
    private String url;
    private String username;
    private String password;
    private String tableName;
    private Connection connection;
    private Statement statement;

    @Override
    public void init(Configuration tableConf, StructType tableSchema) {
        LOGGER.info("init jdbc sink with config: {}, \n schema: {}", tableConf, tableSchema);
        this.tableConf = tableConf;
        this.schema = tableSchema;

        this.driver = tableConf.getString(JDBCConfigKeys.GEAFLOW_DSL_JDBC_DRIVER);
        this.url = tableConf.getString(JDBCConfigKeys.GEAFLOW_DSL_JDBC_URL);
        this.username = tableConf.getString(JDBCConfigKeys.GEAFLOW_DSL_JDBC_USERNAME);
        this.password = tableConf.getString(JDBCConfigKeys.GEAFLOW_DSL_JDBC_PASSWORD);
        this.tableName = tableConf.getString(JDBCConfigKeys.GEAFLOW_DSL_JDBC_TABLE_NAME);
    }

    @Override
    public void open(RuntimeContext context) {
        try {
            Class.forName(this.driver);
            this.connection = DriverManager.getConnection(url, username, password);
            this.connection.setAutoCommit(false);
            this.statement = connection.createStatement();
        } catch (Exception e) {
            throw new GeaFlowDSLException("failed to connect to database", e);
        }
    }

    @Override
    public void write(Row row) throws IOException {
        try {
            JDBCUtils.insertIntoTable(this.statement, this.tableName, this.schema.getFields(), row);
        } catch (SQLException e) {
            throw new GeaFlowDSLException("failed to write to table: " + tableName, e);
        }
    }

    @Override
    public void finish() throws IOException {
        try {
            connection.commit();
        } catch (SQLException e) {
            LOGGER.error("failed to commit", e);
            try {
                connection.rollback();
            } catch (SQLException ex) {
                throw new GeaFlowDSLException("failed to rollback", e);
            }
        }
    }

    @Override
    public void close() {
        try {
            if (this.statement != null) {
                this.statement.close();
                this.statement = null;
            }
            if (this.connection != null) {
                this.connection.close();
                this.connection = null;
            }
            LOGGER.info("close");
        } catch (SQLException e) {
            throw new GeaFlowDSLException("failed to close");
        }
    }
}
