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

import static com.google.common.collect.Maps.fromProperties;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MINUTES;

import com.google.common.primitives.Ints;
import java.net.URI;
import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.geaflow.analytics.service.client.AnalyticsClient;
import org.apache.geaflow.common.exception.GeaflowRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AnalyticsConnection implements Connection {

    private static final Logger LOGGER = LoggerFactory.getLogger(AnalyticsConnection.class);

    private static final String SEPARATOR_COLON = ":";
    private final AtomicReference<Integer> networkTimeoutMillis = new AtomicReference<>(Ints.saturatedCast(MINUTES.toMillis(2)));
    private final Map<String, String> clientInfo = new ConcurrentHashMap<>();
    private final AtomicBoolean closed = new AtomicBoolean();
    private final AtomicReference<String> catalog = new AtomicReference<>();
    private final URI httpUri;
    private final String user;
    private final Map<String, String> sessionProperties;

    private final AnalyticsDriverURI analyticsDriverURI;

    private AnalyticsClient client;

    public static Connection newInstance(String url, Properties properties) {
        AnalyticsDriverURI analyticsDriverURI = new AnalyticsDriverURI(url, properties);
        return new AnalyticsConnection(analyticsDriverURI);
    }

    private AnalyticsConnection(AnalyticsDriverURI uri) {
        requireNonNull(uri, "analytics driver uri is null");
        this.analyticsDriverURI = uri;
        this.user = uri.getUser();
        this.httpUri = uri.getHttpUri();
        this.sessionProperties = new ConcurrentHashMap<>(uri.getSessionProperties());
        initAnalyticsClient();
    }


    private void initAnalyticsClient() {
        String authority = this.analyticsDriverURI.getAuthority();
        try {
            if (authority != null && !authority.isEmpty()) {
                String[] split = authority.split(SEPARATOR_COLON);
                if (split.length == 2) {
                    String host = split[0];
                    int port = Integer.parseInt(split[1]);
                    this.client = AnalyticsClient.builder()
                        .withHost(host).withPort(port).build();
                    LOGGER.info("init geaflow analytics connection with host [{}] and port [{}]",
                        host, port);
                } else {
                    LOGGER.warn("illegal authority: [{}]", authority);
                    throw new GeaflowRuntimeException("illegal authority: " + authority);
                }
            }
        } catch (Throwable e) {
            LOGGER.warn("parse authority from driver uri failed [{}]", authority, e);
            throw new GeaflowRuntimeException("analytics jdbc create client failed");
        }
    }

    @Override
    public Statement createStatement() throws SQLException {
        checkOpen();
        return AnalyticsStatement.newInstance(this, client);
    }

    private void checkOpen() throws SQLException {
        if (isClosed()) {
            throw new GeaflowRuntimeException("Connection is closed");
        }
    }

    @Override
    public PreparedStatement prepareStatement(String sql) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public CallableStatement prepareCall(String sql) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public String nativeSQL(String sql) throws SQLException {
        checkOpen();
        return sql;
    }

    @Override
    public void setAutoCommit(boolean autoCommit) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean getAutoCommit() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void commit() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void rollback() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void close() throws SQLException {
        closed.set(true);
        this.client.shutdown();
    }

    @Override
    public boolean isClosed() throws SQLException {
        return closed.get();
    }

    @Override
    public DatabaseMetaData getMetaData() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setReadOnly(boolean readOnly) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isReadOnly() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setCatalog(String catalog) throws SQLException {
        checkOpen();
        this.catalog.set(catalog);
    }

    @Override
    public String getCatalog() throws SQLException {
        checkOpen();
        return catalog.get();
    }

    @Override
    public void setTransactionIsolation(int level) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getTransactionIsolation() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        checkOpen();
        return null;
    }

    @Override
    public void clearWarnings() throws SQLException {
        checkOpen();
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency)
        throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType,
                                              int resultSetConcurrency) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency)
        throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Map<String, Class<?>> getTypeMap() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setHoldability(int holdability) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getHoldability() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Savepoint setSavepoint() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Savepoint setSavepoint(String name) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void rollback(Savepoint savepoint) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void releaseSavepoint(Savepoint savepoint) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency,
                                     int resultSetHoldability) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType,
                                              int resultSetConcurrency, int resultSetHoldability)
        throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency,
                                         int resultSetHoldability) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys)
        throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public PreparedStatement prepareStatement(String sql, String[] columnNames)
        throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Clob createClob() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Blob createBlob() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public NClob createNClob() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public SQLXML createSQLXML() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isValid(int timeout) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setClientInfo(String name, String value) throws SQLClientInfoException {
        requireNonNull(name, "name is null");
        if (value != null) {
            clientInfo.put(name, value);
        } else {
            clientInfo.remove(name);
        }
    }

    @Override
    public void setClientInfo(Properties properties) throws SQLClientInfoException {
        clientInfo.putAll(fromProperties(properties));
    }

    @Override
    public String getClientInfo(String name) throws SQLException {
        return clientInfo.get(name);
    }

    @Override
    public Properties getClientInfo() throws SQLException {
        Properties properties = new Properties();
        for (Map.Entry<String, String> entry : clientInfo.entrySet()) {
            properties.setProperty(entry.getKey(), entry.getValue());
        }
        return properties;
    }

    @Override
    public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setSchema(String schema) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getSchema() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void abort(Executor executor) throws SQLException {
        close();
    }

    @Override
    public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
        checkOpen();
        if (milliseconds < 0) {
            throw new GeaflowRuntimeException("NetWork timeout is negative");
        }
        networkTimeoutMillis.set(milliseconds);
    }

    @Override
    public int getNetworkTimeout() throws SQLException {
        checkOpen();
        return networkTimeoutMillis.get();
    }

    @Override
    public <T> T unwrap(Class<T> iFace) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isWrapperFor(Class<?> iFace) throws SQLException {
        throw new UnsupportedOperationException();
    }
}
