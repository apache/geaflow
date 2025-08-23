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
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Stream;
import org.apache.geaflow.api.context.RuntimeContext;
import org.apache.geaflow.api.window.WindowType;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.dsl.common.data.Row;
import org.apache.geaflow.dsl.common.exception.GeaFlowDSLException;
import org.apache.geaflow.dsl.common.types.StructType;
import org.apache.geaflow.dsl.common.types.TableSchema;
import org.apache.geaflow.dsl.connector.api.FetchData;
import org.apache.geaflow.dsl.connector.api.Offset;
import org.apache.geaflow.dsl.connector.api.Partition;
import org.apache.geaflow.dsl.connector.api.TableSource;
import org.apache.geaflow.dsl.connector.api.serde.DeserializerFactory;
import org.apache.geaflow.dsl.connector.api.serde.TableDeserializer;
import org.apache.geaflow.dsl.connector.api.window.FetchWindow;
import org.apache.geaflow.dsl.connector.jdbc.util.JDBCUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JDBCTableSource implements TableSource {

    private static final Logger LOGGER = LoggerFactory.getLogger(JDBCTableSource.class);
    private Configuration tableConf;
    private StructType schema;
    private String driver;
    private String url;
    private String username;
    private String password;
    private String tableName;
    private long partitionNum;
    private String partitionColumn;
    private long lowerBound;
    private long upperBound;
    private Map<Partition, Connection> partitionConnectionMap = new HashMap<>();
    private Map<Partition, Statement> partitionStatementMap = new HashMap<>();

    @Override
    public void init(Configuration tableConf, TableSchema tableSchema) {
        LOGGER.info("prepare with config: {}, \n schema: {}", tableConf, tableSchema);
        this.tableConf = tableConf;
        this.schema = tableSchema;

        this.driver = tableConf.getString(JDBCConfigKeys.GEAFLOW_DSL_JDBC_DRIVER);
        this.url = tableConf.getString(JDBCConfigKeys.GEAFLOW_DSL_JDBC_URL);
        this.username = tableConf.getString(JDBCConfigKeys.GEAFLOW_DSL_JDBC_USERNAME);
        this.password = tableConf.getString(JDBCConfigKeys.GEAFLOW_DSL_JDBC_PASSWORD);
        this.tableName = tableConf.getString(JDBCConfigKeys.GEAFLOW_DSL_JDBC_TABLE_NAME);
        this.partitionNum = tableConf.getLong(JDBCConfigKeys.GEAFLOW_DSL_JDBC_PARTITION_NUM);
        if (this.partitionNum <= 0) {
            throw new GeaFlowDSLException("Invalid partition number: {}", partitionNum);
        }
        this.partitionColumn =
            tableConf.getString(JDBCConfigKeys.GEAFLOW_DSL_JDBC_PARTITION_COLUMN);
        this.lowerBound = tableConf.getLong(JDBCConfigKeys.GEAFLOW_DSL_JDBC_PARTITION_LOWERBOUND);
        this.upperBound = tableConf.getLong(JDBCConfigKeys.GEAFLOW_DSL_JDBC_PARTITION_UPPERBOUND);
        // PartitionColumn, lowerbound and upperbound must all be specified. Otherwise, ignore.
        if (Stream.of(partitionColumn, lowerBound, upperBound).allMatch(Objects::nonNull)) {
            if (partitionNum == 1) {
                // create one connection and ignore partitionColumn, lowerbound, upperbound.
            } else if (partitionNum > 1 && lowerBound >= upperBound) {
                throw new GeaFlowDSLException("Upperbound must greater than lowerbound"
                    + "(lowerbound:%d upperbound:%d).", lowerBound, upperBound);
            } else {
                partitionNum = Math.min(upperBound - lowerBound, partitionNum);
            }
        }
    }

    @Override
    public void open(RuntimeContext context) {
        try {
            Class.forName(this.driver);
        } catch (ClassNotFoundException e) {
            throw new GeaFlowDSLException("failed to load driver: {}.", this.driver);
        }
    }

    @Override
    public List<Partition> listPartitions() {
        if (partitionNum <= 0) {
            throw new GeaFlowDSLException("Invalid partition number: {}", partitionNum);
        }

        if (partitionNum == 1) {
            return Collections.singletonList(new JDBCPartition(tableName, ""));
        }

        long stride = upperBound / partitionNum - lowerBound / partitionNum;
        long currentValue = lowerBound;
        List<Partition> partitions = new ArrayList<>();
        for (long i = 0; i < partitionNum; ++i) {
            String lBound = i != 0 ? String.format("%s >= %d", partitionColumn, currentValue) :
                null;
            currentValue += stride;
            String uBound = i != partitionNum - 1 ? String.format("%s < %d", partitionColumn,
                currentValue) : null;
            String whereClause;
            if (uBound == null) {
                whereClause = lBound;
            } else if (lBound == null) {
                whereClause = String.format("%s OR %s IS NULL", uBound, partitionColumn);
            } else {
                whereClause = String.format("%s AND %s", lBound, uBound);
            }
            whereClause = "WHERE " + whereClause;
            partitions.add(new JDBCPartition(tableName, whereClause));
        }
        return partitions;
    }

    @Override
    public List<Partition> listPartitions(int parallelism) {
        return listPartitions();
    }

    @Override
    public <IN> TableDeserializer<IN> getDeserializer(Configuration conf) {
        return DeserializerFactory.loadRowTableDeserializer();
    }

    @Override
    public <T> FetchData<T> fetch(Partition partition, Optional<Offset> startOffset,
                                  FetchWindow windowInfo) throws IOException {
        if (!(windowInfo.getType() == WindowType.SIZE_TUMBLING_WINDOW
            || windowInfo.getType() == WindowType.ALL_WINDOW)) {
            throw new GeaFlowDSLException("Not support window type:{}", windowInfo.getType());
        }
        JDBCPartition jdbcPartition = (JDBCPartition) partition;
        if (!jdbcPartition.getTableName().equals(this.tableName)) {
            throw new GeaFlowDSLException("wrong partition");
        }
        Statement statement = partitionStatementMap.get(partition);
        if (statement == null) {
            try {
                Connection connection = DriverManager.getConnection(url, username, password);
                statement = connection.createStatement();
                partitionConnectionMap.put(partition, connection);
                partitionStatementMap.put(partition, statement);
            } catch (SQLException e) {
                throw new GeaFlowDSLException("failed to connect.");
            }
        }

        long offset = 0;
        if (startOffset.isPresent()) {
            offset = startOffset.get().getOffset();
        }
        List<Row> dataList;
        try {
            dataList = JDBCUtils.selectRowsFromTable(statement, this.tableName,
                jdbcPartition.getWhereClause(), this.schema.size(), offset, windowInfo.windowSize(), this.schema.getField(0).getName());
        } catch (SQLException e) {
            throw new GeaFlowDSLException("select rows form table failed.", e);
        }
        JDBCOffset nextOffset = new JDBCOffset(offset + dataList.size());
        boolean isFinish = windowInfo.getType() == WindowType.ALL_WINDOW || dataList.size() < windowInfo.windowSize();
        return (FetchData<T>) FetchData.createStreamFetch(dataList, nextOffset, isFinish);
    }

    @Override
    public void close() {
        try {
            for (Statement statement : this.partitionStatementMap.values()) {
                if (statement != null) {
                    statement.close();
                }
            }
            this.partitionStatementMap.clear();
            for (Connection connection : this.partitionConnectionMap.values()) {
                if (connection != null) {
                    connection.close();
                }
            }
            this.partitionConnectionMap.clear();
        } catch (SQLException e) {
            throw new GeaFlowDSLException("failed to close connection.");
        }
    }

    public static class JDBCPartition implements Partition {

        String tableName;
        String whereClause;

        public JDBCPartition(String tableName, String whereClause) {
            this.tableName = tableName;
            this.whereClause = whereClause;
        }

        public String getTableName() {
            return tableName;
        }

        public String getWhereClause() {
            return whereClause;
        }

        @Override
        public String getName() {
            if (whereClause == null || whereClause.isEmpty()) {
                return tableName;
            } else {
                return tableName + "-" + whereClause;
            }
        }

        @Override
        public int hashCode() {
            return Objects.hash(tableName, whereClause);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof JDBCPartition)) {
                return false;
            }
            JDBCPartition that = (JDBCPartition) o;
            return Objects.equals(tableName, that.tableName) && Objects.equals(whereClause,
                that.whereClause);
        }

    }

    public static class JDBCOffset implements Offset {

        private final long offset;

        public JDBCOffset(long offset) {
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
