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

package org.apache.geaflow.store.jdbc;

import com.google.common.base.Joiner;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.common.exception.GeaflowRuntimeException;
import org.apache.geaflow.common.utils.GsonUtil;
import org.apache.geaflow.common.utils.RetryCommand;
import org.apache.geaflow.store.IBaseStore;
import org.apache.geaflow.store.context.StoreContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class BaseJdbcStore implements IBaseStore {

    private static final Logger LOGGER = LoggerFactory.getLogger(BaseJdbcStore.class);
    private static final char SQL_SEPARATOR = ',';

    private static HikariDataSource ds;

    protected String tableName;
    protected Configuration config;
    protected int retries;
    protected String pk;
    private String updateFormat;
    private String insertFormat;
    private String deleteFormat;
    private String queryFormat;

    public void init(StoreContext storeContext) {
        this.tableName = storeContext.getName();
        this.config = storeContext.getConfig();
        this.retries = this.config.getInteger(JdbcConfigKeys.JDBC_MAX_RETRIES);
        this.pk = this.config.getString(JdbcConfigKeys.JDBC_PK);
        this.insertFormat = this.config.getString(JdbcConfigKeys.JDBC_INSERT_FORMAT);
        this.updateFormat = this.config.getString(JdbcConfigKeys.JDBC_UPDATE_FORMAT);
        this.deleteFormat = this.config.getString(JdbcConfigKeys.JDBC_DELETE_FORMAT);
        this.queryFormat = this.config.getString(JdbcConfigKeys.JDBC_QUERY_FORMAT);
        initConnectionPool(config);
    }

    private synchronized void initConnectionPool(Configuration config) {
        if (ds != null) {
            return;
        }
        HikariConfig conf = new HikariConfig();
        conf.setJdbcUrl(config.getString(JdbcConfigKeys.JDBC_URL));
        conf.setUsername(config.getString(JdbcConfigKeys.JDBC_USER_NAME));
        conf.setPassword(config.getString(JdbcConfigKeys.JDBC_PASSWORD));
        conf.setDriverClassName(config.getString(JdbcConfigKeys.JDBC_DRIVER_CLASS));
        conf.setMaximumPoolSize(config.getInteger(JdbcConfigKeys.JDBC_CONNECTION_POOL_SIZE));
        String jsonConfig = this.config.getString(JdbcConfigKeys.JSON_CONFIG);
        Map<String, String> map = GsonUtil.parse(jsonConfig);
        for (Entry<String, String> entry : map.entrySet()) {
            conf.addDataSourceProperty(entry.getKey(), entry.getValue());
        }
        ds = new HikariDataSource(conf);
    }

    protected boolean insert(String key, String[] columns, Object[] values) throws SQLException {
        if (columns.length != values.length) {
            throw new GeaflowRuntimeException("columns' size does not match values'");
        }

        String sql = String.format(insertFormat, this.tableName, this.pk,
            Joiner.on(SQL_SEPARATOR).join(columns),
            Joiner.on(SQL_SEPARATOR).join(Collections.nCopies(values.length, "?")));
        try (Connection conn = ds.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, key);
            for (int i = 0; i < values.length; i++) {
                ps.setObject(i + 2, values[i]);
            }
            return 0 != RetryCommand.run(ps::executeUpdate, this.retries);
        }

    }

    protected boolean update(String key, String[] columns, Object[] values) throws SQLException {
        if (columns.length != values.length) {
            throw new GeaflowRuntimeException("columns' size does not match values'");
        }

        String sql = String.format(updateFormat,
            this.tableName, Joiner.on("=?, ").join(columns), pk, key);

        try (Connection conn = ds.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            for (int i = 0; i < values.length; i++) {
                ps.setObject(i + 1, values[i]);
            }
            return 0 != RetryCommand.run(ps::executeUpdate, this.retries);
        }
    }

    protected byte[] query(String key, String[] columns) throws SQLException {
        String selectColumn = columns == null ? "*" : Joiner.on(SQL_SEPARATOR).join(columns);
        String sql = String.format(queryFormat, selectColumn, this.tableName, pk, key);
        try (Connection conn = ds.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            try (ResultSet rs = RetryCommand.run(ps::executeQuery, this.retries)) {
                if (rs.next()) {
                    return rs.getBytes(1);
                } else {
                    return null;
                }
            }
        }
    }

    protected boolean delete(String key) throws SQLException {
        String sql = String.format(deleteFormat, this.tableName, pk, key);
        try (Connection conn = ds.getConnection(); PreparedStatement ps = conn.prepareStatement(
            sql)) {
            return 0 != RetryCommand.run(ps::executeUpdate, this.retries);
        }
    }

    @Override
    public void flush() {
        LOGGER.info("flush");
    }

    @Override
    public synchronized void close() {
        if (!ds.isClosed()) {
            ds.close();
        }
    }
}
