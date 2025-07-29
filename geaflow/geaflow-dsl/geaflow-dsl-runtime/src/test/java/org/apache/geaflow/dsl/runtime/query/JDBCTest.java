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

package org.apache.geaflow.dsl.runtime.query;

import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import org.apache.geaflow.common.config.keys.DSLConfigKeys;
import org.apache.geaflow.dsl.runtime.testenv.SourceFunctionNoPartitionCheck;
import org.h2.jdbcx.JdbcDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class JDBCTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(JDBCTest.class);

    private final String URL = "jdbc:h2:mem:testdb;DB_CLOSE_DELAY=-1";
    private final String username = "h2_user";
    private final String password = "h2_pwd";

    @BeforeClass
    public void setup() throws SQLException {
        LOGGER.info("start h2 database.");
        JdbcDataSource dataSource = new JdbcDataSource();
        dataSource.setURL(URL);
        dataSource.setUser(username);
        dataSource.setPassword(password);

        Statement statement = dataSource.getConnection().createStatement();
        statement.execute("CREATE TABLE test (user_name VARCHAR(255) primary key, count INT);");
        statement.execute("CREATE TABLE users (id INT primary key, name VARCHAR(255), age INT);");
    }

    @Test
    public void testJDBC_001() throws Exception {
        Map<String, String> config = new HashMap<>();
        config.put(DSLConfigKeys.GEAFLOW_DSL_WINDOW_SIZE.getKey(), String.valueOf(1L));
        config.put(DSLConfigKeys.GEAFLOW_DSL_CUSTOM_SOURCE_FUNCTION.getKey(),
            SourceFunctionNoPartitionCheck.class.getName());
        QueryTester tester = QueryTester
            .build()
            .withQueryPath("/query/jdbc_write_001.sql")
            .withConfig(config)
            .withTestTimeWaitSeconds(60)
            .execute()
            .withQueryPath("/query/jdbc_scan_001.sql")
            .withConfig(config)
            .withTestTimeWaitSeconds(60)
            .execute();

        tester.checkSinkResult();
    }

    @Test
    public void testJDBC_002() throws Exception {
        QueryTester tester = QueryTester
            .build()
            .withQueryPath("/query/jdbc_write_002.sql")
            .withTestTimeWaitSeconds(60)
            .execute()
            .withQueryPath("/query/jdbc_scan_002.sql")
            .withTestTimeWaitSeconds(60)
            .execute();
        tester.checkSinkResult();
    }
}
