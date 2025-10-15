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

package org.apache.geaflow.analytics.service.client.jdbc;

import static java.util.Objects.requireNonNull;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.geaflow.analytics.service.client.AnalyticsClient;
import org.apache.geaflow.analytics.service.query.QueryResults;
import org.apache.geaflow.common.exception.GeaflowRuntimeException;

public class AnalyticsStatement implements Statement {

    private final AtomicReference<AnalyticsConnection> connection;
    private final AtomicInteger queryTimeoutSeconds = new AtomicInteger();
    private final AtomicReference<ResultSet> currentResult = new AtomicReference<>();
    private final AtomicReference<AnalyticsClient> executingClient;

    private AnalyticsStatement(AnalyticsConnection connection, AnalyticsClient client) {
        this.connection = new AtomicReference<>(requireNonNull(connection, "analytics connection "
            + "is null"));
        this.executingClient = new AtomicReference<>(requireNonNull(client, "analytics client "
            + "is null"));
    }

    protected static AnalyticsStatement newInstance(AnalyticsConnection connection,
                                                    AnalyticsClient client) {
        return new AnalyticsStatement(connection, client);
    }

    @Override
    public ResultSet executeQuery(String script) throws SQLException {
        if (!execute(script)) {
            throw new GeaflowRuntimeException("Execute statement is not a query: " + script);
        }
        return currentResult.get();
    }

    @Override
    public boolean execute(String statement) throws SQLException {
        return executeQueryInternal(statement);
    }

    @Override
    public int executeUpdate(String query) {
        return 0;
    }

    @Override
    public void close() throws SQLException {
        if (executingClient.get() != null) {
            executingClient.get().shutdown();
        }
        if (connection.get() != null) {
            connection.get().close();
        }
    }

    private boolean executeQueryInternal(String script) throws SQLException {
        // Remove current results.
        removeCurrentResults();
        // Check connection.
        checkConnect();
        AnalyticsClient client = executingClient.get();
        ResultSet resultSet = null;
        try {
            QueryResults queryResults = client.executeQuery(script);
            resultSet = new AnalyticsResultSet(this, queryResults);
            executingClient.set(client);
            currentResult.set(resultSet);
            return true;
        } catch (Exception e) {
            throw new GeaflowRuntimeException("execute query fail", e);
        } finally {
            executingClient.set(null);
            if (currentResult.get() == null) {
                if (resultSet != null) {
                    resultSet.close();
                }
                if (client != null) {
                    client.shutdown();
                }
            }
        }
    }

    private void removeCurrentResults() throws SQLException {
        ResultSet resultSet = currentResult.getAndSet(null);
        if (resultSet != null) {
            resultSet.close();
        }
    }

    private void checkConnect() throws SQLException {
        connection();
    }

    private AnalyticsConnection connection() throws SQLException {
        AnalyticsConnection connection = this.connection.get();
        if (connection == null || connection.isClosed()) {
            throw new GeaflowRuntimeException("analytics connection is closed");
        }
        return connection;
    }


    @Override
    public int getMaxFieldSize() throws SQLException {
        return 0;
    }

    @Override
    public void setMaxFieldSize(int max) throws SQLException {

    }

    @Override
    public int getMaxRows() throws SQLException {
        return 0;
    }

    @Override
    public void setMaxRows(int max) throws SQLException {

    }

    @Override
    public void setEscapeProcessing(boolean enable) throws SQLException {

    }

    @Override
    public int getQueryTimeout() throws SQLException {
        checkConnect();
        return this.queryTimeoutSeconds.get();
    }

    @Override
    public void setQueryTimeout(int seconds) throws SQLException {
        checkConnect();
        if (seconds < 0) {
            throw new GeaflowRuntimeException("Query timeout seconds must be positive");
        }
        queryTimeoutSeconds.set(seconds);
    }

    @Override
    public void cancel() throws SQLException {
        checkConnect();
        AnalyticsClient analyticsClient = executingClient.get();
        removeCurrentResults();
        if (analyticsClient != null) {
            analyticsClient.shutdown();
        }
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void clearWarnings() throws SQLException {

    }

    @Override
    public void setCursorName(String name) throws SQLException {

    }

    @Override
    public ResultSet getResultSet() throws SQLException {
        return this.currentResult.get();
    }

    @Override
    public int getUpdateCount() throws SQLException {
        return 0;
    }

    @Override
    public boolean getMoreResults() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setFetchDirection(int direction) throws SQLException {

    }

    @Override
    public int getFetchDirection() throws SQLException {
        return 0;
    }

    @Override
    public void setFetchSize(int rows) throws SQLException {

    }

    @Override
    public int getFetchSize() throws SQLException {
        return 0;
    }

    @Override
    public int getResultSetConcurrency() throws SQLException {
        return 0;
    }

    @Override
    public int getResultSetType() throws SQLException {
        return 0;
    }

    @Override
    public void addBatch(String query) throws SQLException {

    }

    @Override
    public void clearBatch() throws SQLException {

    }

    @Override
    public int[] executeBatch() throws SQLException {
        return new int[0];
    }

    @Override
    public Connection getConnection() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean getMoreResults(int current) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public ResultSet getGeneratedKeys() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public int executeUpdate(String query, int autoGeneratedKeys) throws SQLException {
        return 0;
    }

    @Override
    public int executeUpdate(String query, int[] columnIndexes) throws SQLException {
        return 0;
    }

    @Override
    public int executeUpdate(String query, String[] columnNames) throws SQLException {
        return 0;
    }

    @Override
    public boolean execute(String query, int autoGeneratedKeys) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean execute(String query, int[] columnIndexes) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean execute(String query, String[] columnNames) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getResultSetHoldability() throws SQLException {
        return 0;
    }

    @Override
    public boolean isClosed() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setPoolable(boolean enablePool) throws SQLException {

    }

    @Override
    public boolean isPoolable() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void closeOnCompletion() throws SQLException {

    }

    @Override
    public boolean isCloseOnCompletion() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> T unwrap(Class<T> param) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isWrapperFor(Class<?> param) throws SQLException {
        throw new UnsupportedOperationException();
    }

}
