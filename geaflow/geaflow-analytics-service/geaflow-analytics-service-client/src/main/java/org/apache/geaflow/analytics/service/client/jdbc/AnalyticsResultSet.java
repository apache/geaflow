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

import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

import com.google.common.base.Preconditions;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableMap;
import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.geaflow.analytics.service.query.IQueryStatus;
import org.apache.geaflow.analytics.service.query.QueryError;
import org.apache.geaflow.analytics.service.query.QueryResults;
import org.apache.geaflow.common.exception.GeaflowRuntimeException;
import org.apache.geaflow.common.type.IType;
import org.apache.geaflow.dsl.common.data.Row;
import org.apache.geaflow.dsl.common.data.RowEdge;
import org.apache.geaflow.dsl.common.data.RowVertex;
import org.apache.geaflow.dsl.common.types.EdgeType;
import org.apache.geaflow.dsl.common.types.TableField;
import org.apache.geaflow.dsl.common.types.VertexType;
import org.apache.geaflow.model.graph.edge.IEdge;
import org.apache.geaflow.model.graph.edge.impl.ValueLabelEdge;
import org.apache.geaflow.model.graph.vertex.IVertex;
import org.apache.geaflow.model.graph.vertex.impl.ValueLabelVertex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AnalyticsResultSet implements ResultSet {

    private static final Logger LOGGER = LoggerFactory.getLogger(AnalyticsResultSet.class);

    private final QueryResults queryResults;
    private final Statement statement;
    private final Iterator<List<Object>> dataResults;
    private final AtomicReference<List<Object>> currentResult = new AtomicReference<>();
    private final AtomicBoolean closed = new AtomicBoolean();
    private final AtomicBoolean wasNull = new AtomicBoolean();
    private String queryId;
    private final AnalyticsResultMetaData resultSetMetaData;
    private final Map<String, Integer> fieldLabel2Index;
    private final Map<Integer, String> fieldIndex2Label;

    public AnalyticsResultSet(AnalyticsStatement statement, QueryResults result) {
        requireNonNull(result, "query result is null");
        this.statement = statement;
        this.resultSetMetaData = new AnalyticsResultMetaData(result.getResultMeta());
        this.queryId = result.getQueryId();
        this.queryResults = result;
        this.fieldLabel2Index = getFieldLabel2IndexMap(result.getResultMeta());
        this.fieldIndex2Label = getFieldIndex2LabelMap(result.getResultMeta());
        List<List<Object>> iteratorData = queryResults.getRawData();
        this.dataResults = flatten(new PageIterator(iteratorData), Long.MAX_VALUE);
    }

    public static GeaflowRuntimeException resultsException(IQueryStatus result) {
        QueryError error = requireNonNull(result.getError());
        String message = format("QueryId failed (#%s): %s", result.getQueryId(), error.getName());
        return new GeaflowRuntimeException(message);
    }

    public String getQueryId() {
        return this.queryId;
    }

    @Override
    public boolean next() throws SQLException {
        checkResultSetOpen();
        try {
            if (!dataResults.hasNext()) {
                currentResult.set(null);
                return false;
            }
            currentResult.set(dataResults.next());
            return true;
        } catch (RuntimeException e) {
            if (e.getCause() instanceof SQLException) {
                throw new GeaflowRuntimeException(e);
            }
            throw new GeaflowRuntimeException("fetching results failed", e);
        }
    }

    @Override
    public void close() throws SQLException {
        closed.set(true);
        statement.close();
    }

    @Override
    public boolean wasNull() throws SQLException {
        return wasNull.get();
    }

    @Override
    public String getString(int index) throws SQLException {
        Object value = getFiledByIndex(index);
        return (value != null) ? value.toString() : null;
    }

    @Override
    public boolean getBoolean(int index) throws SQLException {
        Object value = getFiledByIndex(index);
        return (value != null) ? (Boolean) value : false;
    }

    @Override
    public byte getByte(int index) throws SQLException {
        return convertNumber(getFiledByIndex(index)).byteValue();
    }

    @Override
    public short getShort(int index) throws SQLException {
        return convertNumber(getFiledByIndex(index)).shortValue();
    }

    @Override
    public int getInt(int index) throws SQLException {
        return convertNumber(getFiledByIndex(index)).intValue();
    }

    @Override
    public long getLong(int index) throws SQLException {
        return convertNumber(getFiledByIndex(index)).longValue();
    }

    @Override
    public float getFloat(int index) throws SQLException {
        return convertNumber(getFiledByIndex(index)).floatValue();
    }

    @Override
    public double getDouble(int index) throws SQLException {
        return convertNumber(getFiledByIndex(index)).doubleValue();
    }

    @Override
    public BigDecimal getBigDecimal(int index, int scale) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public byte[] getBytes(int index) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Date getDate(int index) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public IVertex<Object, Map<Object, Object>> getVertex(int index) throws SQLException {
        Object field = getObject(index);
        Preconditions.checkState(field instanceof RowVertex, "field index " + index + " must be "
            + "vertex, but get type is " + field.getClass().getCanonicalName());
        RowVertex rowVertex = (RowVertex) field;
        String fieldLabel = getFieldLabelByIndex(index);
        Map<Object, Object> properties = parseVertexValue(rowVertex, fieldLabel);
        return new ValueLabelVertex<>(rowVertex.getId(), properties, rowVertex.getLabel());
    }

    public IVertex<Object, Map<Object, Object>> getVertex(String label) throws SQLException {
        return getVertex(getFieldIndexByLabel(label));
    }

    public IEdge<Object, Map<Object, Object>> getEdge(int index) throws SQLException {
        Object field = getObject(index);
        Preconditions.checkState(field instanceof RowEdge, "field index " + index + " must be "
            + "edge, but get type is " + field.getClass().getCanonicalName());
        RowEdge rowEdge = (RowEdge) field;
        String fieldLabel = getFieldLabelByIndex(index);
        Map<Object, Object> properties = parseEdgeValue(rowEdge, fieldLabel);
        return new ValueLabelEdge<>(rowEdge.getSrcId(), rowEdge.getTargetId(), properties, rowEdge.getDirect(), rowEdge.getLabel());
    }

    public IEdge<Object, Map<Object, Object>> getEdge(String label) throws SQLException {
        return getEdge(getFieldIndexByLabel(label));
    }

    @Override
    public Time getTime(int index) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Timestamp getTimestamp(int index) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public InputStream getAsciiStream(int index) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public InputStream getUnicodeStream(int index) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public InputStream getBinaryStream(int index) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getString(String columnLabel) throws SQLException {
        Object value = getFiledByLabel(columnLabel);
        return (value != null) ? value.toString() : null;
    }

    @Override
    public boolean getBoolean(String columnLabel) throws SQLException {
        Object value = getFiledByLabel(columnLabel);
        return (value != null) ? (Boolean) value : false;
    }

    @Override
    public byte getByte(String columnLabel) throws SQLException {
        return convertNumber(getFiledByLabel(columnLabel)).byteValue();
    }

    @Override
    public short getShort(String columnLabel) throws SQLException {
        return convertNumber(getFiledByLabel(columnLabel)).shortValue();
    }

    @Override
    public int getInt(String columnLabel) throws SQLException {
        return convertNumber(getFiledByLabel(columnLabel)).intValue();
    }

    @Override
    public long getLong(String columnLabel) throws SQLException {
        return convertNumber(getFiledByLabel(columnLabel)).longValue();
    }

    @Override
    public float getFloat(String columnLabel) throws SQLException {
        return convertNumber(getFiledByLabel(columnLabel)).floatValue();
    }

    @Override
    public double getDouble(String columnLabel) throws SQLException {
        return convertNumber(getFiledByLabel(columnLabel)).doubleValue();
    }

    @Override
    public BigDecimal getBigDecimal(String columnLabel, int scale) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public byte[] getBytes(String columnLabel) throws SQLException {
        return (byte[]) getFiledByLabel(columnLabel);
    }

    @Override
    public Date getDate(String columnLabel) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Time getTime(String columnLabel) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Timestamp getTimestamp(String columnLabel) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public InputStream getAsciiStream(String columnLabel) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public InputStream getUnicodeStream(String columnLabel) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public InputStream getBinaryStream(String columnLabel) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void clearWarnings() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getCursorName() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object getObject(int columnIndex) throws SQLException {
        return getFiledByIndex(columnIndex);
    }

    @Override
    public Object getObject(String columnLabel) throws SQLException {
        return getObject(getFieldIndexByLabel(columnLabel));
    }


    @Override
    public int findColumn(String columnLabel) throws SQLException {
        checkResultSetOpen();
        return getFieldIndexByLabel(columnLabel);
    }

    @Override
    public Reader getCharacterStream(int index) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Reader getCharacterStream(String columnLabel) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public BigDecimal getBigDecimal(int index) throws SQLException {
        Object value = getFiledByIndex(index);
        if (value == null) {
            return null;
        }
        return new BigDecimal(String.valueOf(value));
    }

    @Override
    public BigDecimal getBigDecimal(String columnLabel) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isBeforeFirst() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isAfterLast() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isFirst() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isLast() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void beforeFirst() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void afterLast() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean first() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean last() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getRow() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean absolute(int row) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean relative(int rows) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean previous() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setFetchDirection(int direction) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getFetchDirection() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setFetchSize(int rows) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getFetchSize() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getType() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getConcurrency() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean rowUpdated() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean rowInserted() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean rowDeleted() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateNull(int index) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateBoolean(int index, boolean x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateByte(int index, byte x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateShort(int index, short x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateInt(int index, int x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateLong(int index, long x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateFloat(int index, float x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateDouble(int index, double x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateBigDecimal(int index, BigDecimal x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateString(int index, String x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateBytes(int index, byte[] x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateDate(int index, Date x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateTime(int index, Time x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateTimestamp(int index, Timestamp x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateAsciiStream(int index, InputStream x, int length) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateBinaryStream(int index, InputStream x, int length) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateCharacterStream(int index, Reader x, int length) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateObject(int index, Object x, int scaleOrLength) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateObject(int index, Object x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateNull(String columnLabel) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateBoolean(String columnLabel, boolean x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateByte(String columnLabel, byte x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateShort(String columnLabel, short x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateInt(String columnLabel, int x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateLong(String columnLabel, long x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateFloat(String columnLabel, float x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateDouble(String columnLabel, double x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateBigDecimal(String columnLabel, BigDecimal x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateString(String columnLabel, String x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateBytes(String columnLabel, byte[] x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateDate(String columnLabel, Date x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateTime(String columnLabel, Time x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateTimestamp(String columnLabel, Timestamp x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x, int length)
        throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x, int length)
        throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader, int length)
        throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateObject(String columnLabel, Object x, int scaleOrLength) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateObject(String columnLabel, Object x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void insertRow() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateRow() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void deleteRow() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void refreshRow() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void cancelRowUpdates() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void moveToInsertRow() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void moveToCurrentRow() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Statement getStatement() throws SQLException {
        return this.statement;
    }

    @Override
    public Object getObject(int index, Map<String, Class<?>> map) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Ref getRef(int index) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Blob getBlob(int index) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Clob getClob(int index) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Array getArray(int index) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object getObject(String columnLabel, Map<String, Class<?>> map) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Ref getRef(String columnLabel) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Blob getBlob(String columnLabel) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Clob getClob(String columnLabel) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Array getArray(String columnLabel) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Date getDate(int index, Calendar cal) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Date getDate(String columnLabel, Calendar cal) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Time getTime(int index, Calendar cal) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Time getTime(String columnLabel, Calendar cal) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Timestamp getTimestamp(int index, Calendar cal) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Timestamp getTimestamp(String columnLabel, Calendar cal) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public URL getURL(int index) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public URL getURL(String columnLabel) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateRef(int index, Ref x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateRef(String columnLabel, Ref x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateBlob(int index, Blob x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateBlob(String columnLabel, Blob x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateClob(int index, Clob x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateClob(String columnLabel, Clob x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateArray(int index, Array x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateArray(String columnLabel, Array x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public RowId getRowId(int index) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public RowId getRowId(String columnLabel) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateRowId(int index, RowId x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateRowId(String columnLabel, RowId x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getHoldability() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isClosed() throws SQLException {
        return closed.get();
    }

    @Override
    public void updateNString(int index, String nString) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateNString(String columnLabel, String nString) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateNClob(int index, NClob nClob) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateNClob(String columnLabel, NClob nClob) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public NClob getNClob(int index) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public NClob getNClob(String columnLabel) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public SQLXML getSQLXML(int index) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public SQLXML getSQLXML(String columnLabel) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateSQLXML(int index, SQLXML xmlObject) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateSQLXML(String columnLabel, SQLXML xmlObject) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getNString(int index) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getNString(String columnLabel) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Reader getNCharacterStream(int index) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Reader getNCharacterStream(String columnLabel) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateNCharacterStream(int index, Reader x, long length) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateNCharacterStream(String columnLabel, Reader reader, long length)
        throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateAsciiStream(int index, InputStream x, long length) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateBinaryStream(int index, InputStream x, long length) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateCharacterStream(int index, Reader x, long length) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x, long length)
        throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x, long length)
        throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader, long length)
        throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateBlob(int index, InputStream inputStream, long length) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateBlob(String columnLabel, InputStream inputStream, long length)
        throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateClob(int index, Reader reader, long length) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateClob(String columnLabel, Reader reader, long length) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateNClob(int index, Reader reader, long length) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateNClob(String columnLabel, Reader reader, long length) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateNCharacterStream(int index, Reader x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateNCharacterStream(String columnLabel, Reader reader) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateAsciiStream(int index, InputStream x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateBinaryStream(int index, InputStream x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateCharacterStream(int index, Reader x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateBlob(int index, InputStream inputStream) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateBlob(String columnLabel, InputStream inputStream) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateClob(int index, Reader reader) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateClob(String columnLabel, Reader reader) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateNClob(int index, Reader reader) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateNClob(String columnLabel, Reader reader) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> T getObject(int index, Class<T> type) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> T getObject(String columnLabel, Class<T> type) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> T unwrap(Class<T> iFace) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isWrapperFor(Class<?> iFace) throws SQLException {
        throw new UnsupportedOperationException();
    }

    private void checkResultSetOpen() throws SQLException {
        if (isClosed()) {
            throw new SQLException("Analytics result set is closed");
        }
    }

    private int getFieldIndexByLabel(String label) throws SQLException {
        if (label == null) {
            throw new SQLException("field label is null");
        }
        Integer index = fieldLabel2Index.get(label.toLowerCase(ENGLISH));
        if (index == null) {
            throw new SQLException("invalid field label: " + label);
        }
        return index;
    }

    private String getFieldLabelByIndex(int index) throws SQLException {
        if (index <= 0) {
            throw new SQLException(String.format("field index must be positive，but current index is %d",
                index));
        }
        String label = fieldIndex2Label.get(index);
        if (label == null) {
            throw new SQLException(String.format("label is not exist, index is %d", index));
        }
        return label;
    }

    private static Number convertNumber(Object value) throws SQLException {
        if (value == null) {
            return 0;
        }
        if (value instanceof Number) {
            return (Number) value;
        }
        if (value instanceof Boolean) {
            return ((Boolean) value) ? 1 : 0;
        }
        throw new SQLException(
            String.format("value %s is not number", value.getClass().getCanonicalName()));
    }

    private Object getFiledByIndex(int index) throws SQLException {
        checkResultSetOpen();
        checkValidResult();
        if ((index <= 0) || (index > resultSetMetaData.getColumnCount())) {
            throw new SQLException("Invalid column index: " + index);
        }
        Object value = currentResult.get().get(index - 1);
        wasNull.set(value == null);
        return value;
    }

    private Object getFiledByLabel(String label) throws SQLException {
        checkResultSetOpen();
        checkValidResult();
        Object value = currentResult.get().get(getFieldIndexByLabel(label) - 1);
        wasNull.set(value == null);
        return value;
    }

    private static Map<String, Integer> getFieldLabel2IndexMap(RelDataType relDataType) {
        Map<String, Integer> map = new HashMap<>();
        List<RelDataTypeField> fieldList = relDataType.getFieldList();
        for (int i = 0; i < fieldList.size(); i++) {
            String name = fieldList.get(i).getName().toLowerCase(ENGLISH);
            if (!map.containsKey(name)) {
                map.put(name, i + 1);
            }
        }
        return ImmutableMap.copyOf(map);
    }

    private Map<Object, Object> parseVertexValue(RowVertex rowVertex, String fieldLabel) {
        Row vertexValue = rowVertex.getValue();
        Map<Object, Object> properties = new HashMap<>();
        if (vertexValue != null) {
            Map<String, IType<?>> tableFieldName2Type = resultSetMetaData.getTableFieldName2Type();
            for (Entry<String, IType<?>> entry : tableFieldName2Type.entrySet()) {
                if (fieldLabel.equalsIgnoreCase(entry.getKey())) {
                    VertexType vertexType = (VertexType) entry.getValue();
                    List<TableField> valueFields = vertexType.getValueFields();
                    for (int i = 0; i < valueFields.size(); i++) {
                        TableField tableField = valueFields.get(i);
                        Object valueField = vertexValue.getField(i, tableField.getType());
                        properties.put(tableField.getName(), valueField);
                    }
                }
            }
        }
        return properties;
    }

    private Map<Object, Object> parseEdgeValue(RowEdge rowEdge, String fieldLabel) {
        Row edgeValue = rowEdge.getValue();
        Map<Object, Object> properties = new HashMap<>();
        if (edgeValue != null) {
            Map<String, IType<?>> tableFieldName2Type = resultSetMetaData.getTableFieldName2Type();
            for (Entry<String, IType<?>> entry : tableFieldName2Type.entrySet()) {
                if (fieldLabel.equalsIgnoreCase(entry.getKey())) {
                    EdgeType edgeType = (EdgeType) entry.getValue();
                    List<TableField> valueFields = edgeType.getValueFields();
                    for (int i = 0; i < valueFields.size(); i++) {
                        TableField tableField = valueFields.get(i);
                        Object valueField = edgeValue.getField(i, tableField.getType());
                        properties.put(tableField.getName(), valueField);
                    }
                }
            }
        }
        return properties;
    }

    private static Map<Integer, String> getFieldIndex2LabelMap(RelDataType relDataType) {
        Map<Integer, String> map = new HashMap<>();
        List<RelDataTypeField> fieldList = relDataType.getFieldList();
        for (int i = 0; i < fieldList.size(); i++) {
            String name = fieldList.get(i).getName().toLowerCase(ENGLISH);
            if (!map.containsValue(name)) {
                map.put(i + 1, name);
            }
        }
        return ImmutableMap.copyOf(map);
    }

    private void checkValidResult() throws SQLException {
        if (currentResult.get() == null) {
            throw new SQLException("Not on a valid row");
        }
    }


    private static <T> Iterator<T> flatten(Iterator<T> rowsIterator, long maxRows) {
        return (maxRows > 0) ? new RowLimitedIterator<>(rowsIterator, maxRows) : rowsIterator;
    }

    private static class RowLimitedIterator<T> implements Iterator<T> {

        private final Iterator<T> iterator;

        private final long maxRows;

        private long cursor;

        public RowLimitedIterator(Iterator<T> iterator, long maxRows) {
            Preconditions.checkState(maxRows >= 0, "max rows is negative");
            this.iterator = iterator;
            this.maxRows = maxRows;
        }

        @Override
        public boolean hasNext() {
            return cursor < maxRows && iterator.hasNext();
        }

        @Override
        public T next() {
            if (!hasNext()) {
                throw new GeaflowRuntimeException("no such element");
            }
            cursor++;
            return iterator.next();
        }
    }

    private static class PageIterator extends AbstractIterator<List<Object>> {

        private final List<List<Object>> queryData;
        private int cursor;

        private PageIterator(List<List<Object>> queryData) {
            this.queryData = queryData;
            this.cursor = 0;
        }

        @Override
        protected List<Object> computeNext() {
            if (cursor < queryData.size()) {
                List<Object> element = queryData.get(cursor);
                cursor++;
                return element;
            } else {
                return endOfData();
            }
        }
    }
}
