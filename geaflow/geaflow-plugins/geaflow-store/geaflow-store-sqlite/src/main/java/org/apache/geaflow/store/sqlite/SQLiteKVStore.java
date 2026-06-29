/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.geaflow.store.sqlite;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.store.api.key.IKVStore;
import org.apache.geaflow.store.context.StoreContext;

public class SQLiteKVStore<K, V> implements IKVStore<K, V> {

    private Connection connection;
    private final String dbName;
    private PreparedStatement putStmt;
    private PreparedStatement getStmt;
    private PreparedStatement deleteStmt;

    public SQLiteKVStore(String storeName, Configuration config) {
        this.dbName = "jdbc:sqlite:geaflow_kv_" + storeName + ".db";
    }

    public void init(StoreContext storeContext) {
        try {
            Class.forName("org.sqlite.JDBC");
            connection = DriverManager.getConnection(dbName);

            try (Statement stmt = connection.createStatement()) {
                String createTableSql = "CREATE TABLE IF NOT EXISTS kv_store (" 
                                        + "k_key TEXT PRIMARY KEY, " 
                                        + "v_value TEXT)";
                stmt.execute(createTableSql);
            }
            
            putStmt = connection.prepareStatement("INSERT OR REPLACE INTO kv_store (k_key, v_value) VALUES (?, ?)");
            getStmt = connection.prepareStatement("SELECT v_value FROM kv_store WHERE k_key = ?");
            deleteStmt = connection.prepareStatement("DELETE FROM kv_store WHERE k_key = ?");

        } catch (Exception e) {
            throw new RuntimeException("Failed to initialize SQLite KV Store", e);
        }
    }

    public void put(K key, V value) {
        try {
            putStmt.setString(1, key.toString());
            putStmt.setString(2, value.toString());
            putStmt.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException("Failed to put key-value in SQLite", e);
        }
    }

    public V get(K key) {
        try {
            getStmt.setString(1, key.toString());
            try (ResultSet rs = getStmt.executeQuery()) {
                if (rs.next()) {
                    return (V) rs.getString("v_value"); 
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException("Failed to get key from SQLite", e);
        }
        return null;
    }

    public void delete(K key) {
        try {
            deleteStmt.setString(1, key.toString());
            deleteStmt.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException("Failed to delete key from SQLite", e);
        }
    }

    public void remove(K key) {
        delete(key);
    }

    public void flush() {
    }

    public void archive(long checkpointId) {
    }

    public void recovery(long checkpointId) {
    }

    public long recoveryLatest() {
        return 0L;
    }

    public void compact() {
    }

    public void drop() {
        try {
            if (connection != null) {
                try (Statement stmt = connection.createStatement()) {
                    stmt.execute("DROP TABLE IF EXISTS kv_store");
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException("Failed to drop kv tables", e);
        }
    }

    public void close() {
        try {
            if (putStmt != null) {
                putStmt.close();
            }
            if (getStmt != null) {
                getStmt.close();
            }
            if (deleteStmt != null) {
                deleteStmt.close();
            }
            if (connection != null) {
                connection.close();
            }
        } catch (SQLException e) {
            throw new RuntimeException("Failed to close SQLite connection", e);
        }
    }
}