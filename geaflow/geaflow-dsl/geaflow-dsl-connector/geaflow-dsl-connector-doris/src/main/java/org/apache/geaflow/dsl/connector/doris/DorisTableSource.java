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
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import org.apache.geaflow.api.context.RuntimeContext;
import org.apache.geaflow.api.window.WindowType;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.dsl.common.data.Row;
import org.apache.geaflow.dsl.common.data.impl.ObjectRow;
import org.apache.geaflow.dsl.common.exception.GeaFlowDSLException;
import org.apache.geaflow.dsl.common.types.StructType;
import org.apache.geaflow.dsl.common.types.TableSchema;
import org.apache.geaflow.dsl.common.util.Windows;
import org.apache.geaflow.dsl.connector.api.FetchData;
import org.apache.geaflow.dsl.connector.api.Offset;
import org.apache.geaflow.dsl.connector.api.Partition;
import org.apache.geaflow.dsl.connector.api.TableSource;
import org.apache.geaflow.dsl.connector.api.serde.DeserializerFactory;
import org.apache.geaflow.dsl.connector.api.serde.TableDeserializer;
import org.apache.geaflow.dsl.connector.api.window.FetchWindow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A Doris table source that reads data through the MySQL protocol exposed by the Doris FE. The
 * data set can be split into several partitions on a numeric column so that partitions are read
 * in parallel by different tasks.
 */
public class DorisTableSource implements TableSource {

    private static final Logger LOGGER = LoggerFactory.getLogger(DorisTableSource.class);

    private static final String MYSQL_DRIVER = "com.mysql.cj.jdbc.Driver";

    private StructType schema;
    private String jdbcUrl;
    private String username;
    private String password;
    private String database;
    private String table;
    private long partitionNum;
    private String partitionColumn;
    private long lowerBound;
    private long upperBound;

    private Map<Partition, Connection> partitionConnectionMap = new HashMap<>();
    private Map<Partition, Statement> partitionStatementMap = new HashMap<>();

    @Override
    public void init(Configuration conf, TableSchema tableSchema) {
        LOGGER.info("init doris source with config: {}, \n schema: {}", conf, tableSchema);
        this.schema = tableSchema;
        this.jdbcUrl = conf.getString(DorisConfigKeys.GEAFLOW_DSL_DORIS_JDBC_URL, "");
        if (jdbcUrl == null || jdbcUrl.trim().isEmpty()) {
            throw new GeaFlowDSLException("Doris jdbc url must be specified for the source.");
        }
        this.username = conf.getString(DorisConfigKeys.GEAFLOW_DSL_DORIS_USERNAME);
        this.password = conf.getString(DorisConfigKeys.GEAFLOW_DSL_DORIS_PASSWORD);
        this.database = conf.getString(DorisConfigKeys.GEAFLOW_DSL_DORIS_DATABASE, "");
        this.table = conf.getString(DorisConfigKeys.GEAFLOW_DSL_DORIS_TABLE);
        this.partitionNum = conf.getLong(DorisConfigKeys.GEAFLOW_DSL_DORIS_SOURCE_PARTITION_NUM);
        if (partitionNum <= 0) {
            throw new GeaFlowDSLException("Invalid doris source partition number: {}", partitionNum);
        }
        this.partitionColumn = conf.getString(DorisConfigKeys.GEAFLOW_DSL_DORIS_SOURCE_PARTITION_COLUMN);
        this.lowerBound = conf.getLong(DorisConfigKeys.GEAFLOW_DSL_DORIS_SOURCE_PARTITION_LOWERBOUND);
        this.upperBound = conf.getLong(DorisConfigKeys.GEAFLOW_DSL_DORIS_SOURCE_PARTITION_UPPERBOUND);
        if (partitionNum > 1 && lowerBound >= upperBound) {
            throw new GeaFlowDSLException("Upperbound must be greater than lowerbound "
                + "(lowerbound:%d upperbound:%d).", lowerBound, upperBound);
        }
    }

    @Override
    public void open(RuntimeContext context) {
        try {
            Class.forName(MYSQL_DRIVER);
        } catch (ClassNotFoundException e) {
            throw new GeaFlowDSLException("failed to load mysql driver for doris source.", e);
        }
    }

    private String qualifiedTable() {
        return database == null || database.isEmpty() ? table : database + "." + table;
    }

    @Override
    public List<Partition> listPartitions() {
        if (partitionNum == 1) {
            return Collections.singletonList(new DorisPartition(qualifiedTable(), ""));
        }
        long span = Math.min(upperBound - lowerBound, partitionNum);
        long stride = (upperBound - lowerBound) / span;
        long currentValue = lowerBound;
        List<Partition> partitions = new ArrayList<>();
        for (long i = 0; i < span; i++) {
            String lBound = i != 0 ? String.format("%s >= %d", partitionColumn, currentValue) : null;
            currentValue += stride;
            String uBound = i != span - 1
                ? String.format("%s < %d", partitionColumn, currentValue) : null;
            String whereClause;
            if (uBound == null) {
                whereClause = lBound;
            } else if (lBound == null) {
                whereClause = String.format("%s OR %s IS NULL", uBound, partitionColumn);
            } else {
                whereClause = String.format("%s AND %s", lBound, uBound);
            }
            partitions.add(new DorisPartition(qualifiedTable(), "WHERE " + whereClause));
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
        DorisPartition dorisPartition = (DorisPartition) partition;
        Statement statement = partitionStatementMap.get(partition);
        if (statement == null) {
            try {
                Connection connection = DriverManager.getConnection(jdbcUrl, username, password);
                statement = connection.createStatement();
                partitionConnectionMap.put(partition, connection);
                partitionStatementMap.put(partition, statement);
            } catch (SQLException e) {
                throw new GeaFlowDSLException("failed to connect to doris.", e);
            }
        }

        long offset = startOffset.map(Offset::getOffset).orElse(0L);
        long windowSize = windowInfo.windowSize();
        if (windowSize == Windows.SIZE_OF_ALL_WINDOW) {
            windowSize = Integer.MAX_VALUE;
        } else if (windowSize <= 0) {
            throw new GeaFlowDSLException("wrong windowSize: {}", windowSize);
        }

        List<Row> dataList;
        try {
            dataList = selectRows(statement, dorisPartition, offset, windowSize);
        } catch (SQLException e) {
            throw new GeaFlowDSLException("select rows from doris table failed.", e);
        }
        DorisOffset nextOffset = new DorisOffset(offset + dataList.size());
        boolean isFinish = windowInfo.getType() == WindowType.ALL_WINDOW
            || dataList.size() < windowInfo.windowSize();
        return (FetchData<T>) FetchData.createStreamFetch(dataList, nextOffset, isFinish);
    }

    private List<Row> selectRows(Statement statement, DorisPartition partition, long offset,
                                 long windowSize) throws SQLException {
        int columnNum = schema.size();
        String orderColumn = schema.getField(0).getName();
        String query = String.format("SELECT * FROM %s %s ORDER BY %s LIMIT %s OFFSET %s",
            partition.getTableName(), partition.getWhereClause(), orderColumn, windowSize, offset);
        List<Row> rowList = new ArrayList<>();
        try (ResultSet resultSet = statement.executeQuery(query)) {
            while (resultSet.next()) {
                Object[] values = new Object[columnNum];
                for (int i = 1; i <= columnNum; i++) {
                    values[i - 1] = resultSet.getObject(i);
                }
                rowList.add(ObjectRow.create(values));
            }
        }
        return rowList;
    }

    @Override
    public void close() {
        try {
            for (Statement statement : partitionStatementMap.values()) {
                if (statement != null) {
                    statement.close();
                }
            }
            partitionStatementMap.clear();
            for (Connection connection : partitionConnectionMap.values()) {
                if (connection != null) {
                    connection.close();
                }
            }
            partitionConnectionMap.clear();
        } catch (SQLException e) {
            throw new GeaFlowDSLException("failed to close doris source connection.", e);
        }
    }

    public static class DorisPartition implements Partition {

        private final String tableName;
        private final String whereClause;

        public DorisPartition(String tableName, String whereClause) {
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
            }
            return tableName + "-" + whereClause;
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
            if (!(o instanceof DorisPartition)) {
                return false;
            }
            DorisPartition that = (DorisPartition) o;
            return Objects.equals(tableName, that.tableName)
                && Objects.equals(whereClause, that.whereClause);
        }
    }

    public static class DorisOffset implements Offset {

        private final long offset;

        public DorisOffset(long offset) {
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
