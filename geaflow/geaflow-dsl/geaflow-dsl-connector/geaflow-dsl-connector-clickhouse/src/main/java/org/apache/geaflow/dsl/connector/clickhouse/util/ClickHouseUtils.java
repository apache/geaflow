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

package org.apache.geaflow.dsl.connector.clickhouse.util;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.geaflow.common.type.IType;
import org.apache.geaflow.common.type.Types;
import org.apache.geaflow.dsl.common.data.Row;
import org.apache.geaflow.dsl.common.data.impl.ObjectRow;
import org.apache.geaflow.dsl.common.exception.GeaFlowDSLException;
import org.apache.geaflow.dsl.common.types.TableField;
import org.apache.geaflow.dsl.common.util.Windows;

public class ClickHouseUtils {

    /**
     * Builds a parameterized batch-insert statement of the form
     * {@code INSERT INTO table (c1, c2, ...) VALUES (?, ?, ...)}. The same prepared statement is
     * reused across the whole batch, which is what lets ClickHouse ingest rows in bulk instead of
     * one round-trip per row.
     */
    public static String buildInsertSql(String tableName, List<TableField> fields) {
        String columns = fields.stream()
            .map(TableField::getName)
            .collect(Collectors.joining(", "));
        String placeholders = fields.stream()
            .map(field -> "?")
            .collect(Collectors.joining(", "));
        return String.format("INSERT INTO %s (%s) VALUES (%s)", tableName, columns, placeholders);
    }

    /**
     * Binds a single row onto the prepared statement parameters, mapping GeaFlow types to JDBC
     * setters. The caller is expected to call {@link PreparedStatement#addBatch()} afterwards.
     */
    public static void bindRow(PreparedStatement statement, List<TableField> fields, Row row)
        throws SQLException {
        for (int i = 0; i < fields.size(); i++) {
            TableField field = fields.get(i);
            IType<?> type = field.getType();
            Object value = row.getField(i, type);
            int paramIndex = i + 1;
            if (value == null) {
                if (!field.isNullable()) {
                    throw new GeaFlowDSLException("field " + field.getName() + " can not be null");
                }
                statement.setObject(paramIndex, null);
                continue;
            }
            switch (type.getName()) {
                case Types.TYPE_NAME_STRING:
                case Types.TYPE_NAME_BINARY_STRING:
                    statement.setString(paramIndex, value.toString());
                    break;
                default:
                    statement.setObject(paramIndex, value);
            }
        }
    }

    /**
     * Reads a window of rows from the table, optionally restricted to a partition's where clause.
     * Mirrors the JDBC connector's paging so a partition can be fetched in bounded windows.
     */
    public static List<Row> selectRowsFromTable(Statement statement, String tableName,
                                                String whereClause, int columnNum, long startOffset,
                                                long windowSize, String orderByColumnName)
        throws SQLException {
        if (windowSize == Windows.SIZE_OF_ALL_WINDOW) {
            windowSize = Integer.MAX_VALUE;
        } else if (windowSize <= 0) {
            throw new GeaFlowDSLException("wrong windowSize");
        }
        String selectQuery = String.format("SELECT * FROM %s %s ORDER BY %s LIMIT %s OFFSET %s",
            tableName, whereClause, orderByColumnName, windowSize, startOffset);
        ResultSet resultSet = statement.executeQuery(selectQuery);
        List<Row> rowList = new ArrayList<>();
        while (resultSet.next()) {
            Object[] values = new Object[columnNum];
            for (int i = 1; i <= columnNum; i++) {
                values[i - 1] = resultSet.getObject(i);
            }
            rowList.add(ObjectRow.create(values));
        }
        resultSet.close();
        return rowList;
    }
}
