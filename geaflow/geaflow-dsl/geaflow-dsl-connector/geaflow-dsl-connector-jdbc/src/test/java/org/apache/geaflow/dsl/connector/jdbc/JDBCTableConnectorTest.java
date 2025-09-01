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

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import org.apache.geaflow.common.type.Types;
import org.apache.geaflow.dsl.common.data.Row;
import org.apache.geaflow.dsl.common.data.impl.ObjectRow;
import org.apache.geaflow.dsl.common.types.TableField;
import org.apache.geaflow.dsl.connector.jdbc.util.JDBCUtils;
import org.h2.jdbcx.JdbcDataSource;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JDBCTableConnectorTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(JDBCTableConnectorTest.class);

    private static final String driver = "org.h2.Driver";
    private static final String URL = "jdbc:h2:mem:testdb;DB_CLOSE_DELAY=-1";
    private static final String username = "h2_user";
    private static final String password = "h2_pwd";
    private static Connection connection;
    private static Statement statement;

    @BeforeClass
    public static void setup() throws SQLException {
        LOGGER.info("start h2 database.");
        JdbcDataSource dataSource = new JdbcDataSource();
        dataSource.setURL(URL);
        dataSource.setUser(username);
        dataSource.setPassword(password);

        connection = java.sql.DriverManager.getConnection(URL);
        statement = connection.createStatement();
        statement.execute("CREATE TABLE test_table (id INT PRIMARY KEY, name VARCHAR(255))");
        statement.execute("INSERT INTO test_table (id, name) VALUES (1, 'Test1')");
        statement.execute("INSERT INTO test_table (id, name) VALUES (2, 'Test2')");
        statement.execute("INSERT INTO test_table (id, name) VALUES (3, 'Test3')");
        statement.execute("INSERT INTO test_table (id, name) VALUES (4, 'Test4')");
    }

    @AfterClass
    public static void cleanup() throws SQLException {
        statement.close();
        connection.close();
    }

    @Test
    public void testCreateTable() throws SQLException {
        List<TableField> tableFieldList = new ArrayList<>();
        tableFieldList.add(new TableField("id", Types.INTEGER, false));
        tableFieldList.add(new TableField("v1", Types.DOUBLE, true));
        tableFieldList.add(new TableField("v2", Types.DOUBLE, true));
        JDBCUtils.createTemporaryTable(statement, "another_table", tableFieldList);
    }

    @Test
    public void testInsertIntoTable() throws SQLException {
        List<TableField> tableFieldList = new ArrayList<>();
        tableFieldList.add(new TableField("id", Types.INTEGER, false));
        tableFieldList.add(new TableField("name", Types.BINARY_STRING, true));
        Row row = ObjectRow.create(new Object[]{5, null});
        JDBCUtils.insertIntoTable(statement, "test_table", tableFieldList, row);
        List<Row> rowList = JDBCUtils.selectRowsFromTable(statement, "test_table",
            "", 2, 0, 20, "id");
        Row resultRow = null;
        for (Row queryRow : rowList) {
            if ((Integer) queryRow.getField(0, Types.INTEGER) == 5) {
                resultRow = queryRow;
                break;
            }
        }
        assert resultRow != null;
        assert resultRow.getField(1, Types.BINARY_STRING) == null;
    }

    @Test
    public void testSelectRowsFromTable1() throws SQLException {
        List<Row> rowList = JDBCUtils.selectRowsFromTable(statement, "test_table", "", 2, 0, 2, "id");
        assert rowList.size() == 2;
    }

    @Test
    public void testSelectRowsFromTable2() throws SQLException {
        List<Row> rowList = JDBCUtils.selectRowsFromTable(statement, "test_table",
            "WHERE id < 2", 2, 0, 3, "id");
        assert rowList.size() == 1;
    }

    @Test
    public void testSelectRowsFromTable3() throws SQLException {
        List<Row> rowList = JDBCUtils.selectRowsFromTable(statement, "test_table",
            "WHERE id < 4", 2, 0, 1, "id");
        assert rowList.size() == 1;
    }
}
