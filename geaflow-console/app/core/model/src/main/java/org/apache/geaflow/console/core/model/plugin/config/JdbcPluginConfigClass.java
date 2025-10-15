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

package org.apache.geaflow.console.core.model.plugin.config;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;
import lombok.Getter;
import lombok.Setter;
import org.apache.geaflow.console.common.util.exception.GeaflowException;
import org.apache.geaflow.console.common.util.exception.GeaflowIllegalException;
import org.apache.geaflow.console.common.util.type.GeaflowPluginType;
import org.apache.geaflow.console.core.model.config.GeaflowConfigKey;
import org.apache.geaflow.console.core.model.config.GeaflowConfigValue;

@Getter
@Setter
public class JdbcPluginConfigClass extends PluginConfigClass {

    public static final String MYSQL_DRIVER_CLASS = "com.mysql.jdbc.Driver";

    @GeaflowConfigKey(value = "geaflow.store.jdbc.driver.class", comment = "i18n.key.jdbc.driver")
    @GeaflowConfigValue(defaultValue = MYSQL_DRIVER_CLASS)
    private String driverClass;

    @GeaflowConfigKey(value = "geaflow.store.jdbc.url", comment = "i18n.key.jdbc.url")
    @GeaflowConfigValue(required = true, defaultValue = "jdbc:mysql://0.0.0.0:3306/geaflow?characterEncoding=utf8"
        + "&autoReconnect=true&useSSL=false")
    private String url;

    @GeaflowConfigKey(value = "geaflow.store.jdbc.user.name", comment = "i18n.key.username")
    @GeaflowConfigValue(required = true, defaultValue = "geaflow")
    private String username;

    @GeaflowConfigKey(value = "geaflow.store.jdbc.password", comment = "i18n.key.password")
    @GeaflowConfigValue(required = true, defaultValue = "geaflow", masked = true)
    private String password;

    @GeaflowConfigKey(value = "geaflow.store.jdbc.max.retries", comment = "i18n.key.retry.times")
    @GeaflowConfigValue(defaultValue = "3")
    private Integer retryTimes;

    @GeaflowConfigKey(value = "geaflow.store.jdbc.connection.pool.size", comment = "i18n.key.connection.pool.size")
    @GeaflowConfigValue(defaultValue = "10")
    private Integer connectionPoolSize;

    @GeaflowConfigKey(value = "geaflow.store.jdbc.connect.config.json", comment = "i18n.key.connection.ext.config.json")
    private String configJson;

    public JdbcPluginConfigClass() {
        super(GeaflowPluginType.JDBC);
    }

    @Override
    public void testConnection() {
        try (Connection connection = createConnection()) {
            try (Statement statement = connection.createStatement()) {
                String testSql = "SELECT 1";
                try (ResultSet resultSet = statement.executeQuery(testSql)) {
                    if (!resultSet.next()) {
                        throw new GeaflowException("No response content of query '{}'", testSql);
                    }
                }
            }
        } catch (Exception e) {
            throw new GeaflowIllegalException("JDBC connection test failed, caused by {}", e.getMessage(), e);
        }
    }

    public Connection createConnection() {
        try {
            ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
            if (this.driverClass != null) {
                classLoader.loadClass(this.driverClass);
            }

            DriverManager.setLoginTimeout(3);
            Properties properties = new Properties();
            properties.setProperty("user", username);
            properties.setProperty("password", password);
            return DriverManager.getConnection(url, properties);

        } catch (Exception e) {
            throw new GeaflowIllegalException("JDBC connection create failed, caused by {}", e.getMessage(), e);
        }
    }
}
