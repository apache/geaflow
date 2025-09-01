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

package org.apache.geaflow.console.core.service;

import com.google.common.base.Preconditions;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import javax.sql.DataSource;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.geaflow.console.common.util.exception.GeaflowException;
import org.apache.geaflow.console.core.model.plugin.config.JdbcPluginConfigClass;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.datasource.init.ScriptUtils;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class DatasourceService implements InitializingBean {

    @Autowired
    private DataSource dataSource;

    @Getter
    private boolean initialized;

    @Override
    public void afterPropertiesSet() throws Exception {
        try (Connection connection = dataSource.getConnection()) {
            try (Statement statement = connection.createStatement()) {
                try (ResultSet resultSet = statement.executeQuery("SELECT database()")) {
                    if (resultSet.next()) {
                        String databaseName = resultSet.getString(1);
                        Preconditions.checkNotNull(databaseName, "No database selected in url");
                    }
                }

                // check database inited
                try (ResultSet resultSet = statement.executeQuery("SHOW TABLES LIKE 'geaflow_%'")) {
                    if (resultSet.next()) {
                        initialized = true;
                    }
                }
            }

            if (!initialized) {
                synchronized (DatasourceService.class) {
                    ScriptUtils.executeSqlScript(connection, new ClassPathResource("datasource.init.sql"));
                    initialized = true;
                }
            }
        }
    }

    public void executeResource(JdbcPluginConfigClass jdbcConfig, String resource) {
        String url = jdbcConfig.getUrl();

        try {
            try (Connection connection = jdbcConfig.createConnection()) {
                ScriptUtils.executeSqlScript(connection, new ClassPathResource(resource));
            }

        } catch (Exception e) {
            throw new GeaflowException("Execute {} on {} failed", resource, url, e);
        }
    }
}
