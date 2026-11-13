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

package org.apache.geaflow.dsl.connector.clickhouse;

import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.common.type.Types;
import org.apache.geaflow.dsl.common.data.impl.ObjectRow;
import org.apache.geaflow.dsl.common.exception.GeaFlowDSLException;
import org.apache.geaflow.dsl.common.types.StructType;
import org.apache.geaflow.dsl.common.types.TableField;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class ClickHouseTableSinkTest {

    @DataProvider(name = "batchSizes")
    public Object[][] batchSizes() {
        return new Object[][]{{1, 7, 7}, {3, 7, 3}, {100, 7, 1}, {3, 0, 0}, {3, 6, 2}};
    }

    @Test(dataProvider = "batchSizes")
    public void batchesRowsIntoExpectedNumberOfDatabaseCalls(int batchSize, int rows, int batches) throws Exception {
        PreparedStatement statement = mock(PreparedStatement.class);
        ClickHouseTableSink sink = sink(batchSize, statement, mock(Connection.class));
        for (int i = 0; i < rows; i++) {
            sink.write(ObjectRow.create(new Object[]{(long) i}));
        }
        sink.finish();
        sink.finish();
        verify(statement, times(rows)).addBatch();
        verify(statement, times(batches)).executeBatch();
        verify(statement, times(batches)).clearBatch();
        sink.close();
    }

    @Test
    public void failedBatchIsNotSilentlyResubmittedByFinish() throws Exception {
        PreparedStatement statement = mock(PreparedStatement.class);
        doThrow(new SQLException("simulated insert failure")).when(statement).executeBatch();
        ClickHouseTableSink sink = sink(1, statement, mock(Connection.class));
        try {
            sink.write(ObjectRow.create(new Object[]{1L}));
            Assert.fail("An insert failure must be reported");
        } catch (GeaFlowDSLException expected) {
            Assert.assertTrue(expected.getCause() instanceof SQLException);
        }
        sink.finish();
        verify(statement, times(1)).executeBatch();
        verify(statement, times(1)).clearBatch();
        sink.close();
    }

    @Test
    public void statementCloseFailureStillClosesConnection() throws Exception {
        PreparedStatement statement = mock(PreparedStatement.class);
        Connection connection = mock(Connection.class);
        doThrow(new SQLException("simulated close failure")).when(statement).close();
        ClickHouseTableSink sink = sink(1, statement, connection);
        try {
            sink.close();
            Assert.fail("A close failure must be reported");
        } catch (GeaFlowDSLException expected) {
            Assert.assertTrue(expected.getCause() instanceof SQLException);
        }
        sink.close();
        verify(connection, times(1)).close();
        verify(statement, times(1)).close();
    }

    private static ClickHouseTableSink sink(int batchSize, PreparedStatement statement,
                                           Connection connection) throws Exception {
        Configuration conf = new Configuration();
        conf.put(ClickHouseConfigKeys.GEAFLOW_DSL_CLICKHOUSE_URL, "jdbc:clickhouse://localhost:8123/default");
        conf.put(ClickHouseConfigKeys.GEAFLOW_DSL_CLICKHOUSE_TABLE_NAME, "events");
        conf.put(ClickHouseConfigKeys.GEAFLOW_DSL_CLICKHOUSE_WRITE_BATCH_SIZE, String.valueOf(batchSize));
        ClickHouseTableSink sink = new ClickHouseTableSink();
        sink.init(conf, new StructType(new TableField("id", Types.LONG, false)));
        setField(sink, "statement", statement);
        setField(sink, "connection", connection);
        return sink;
    }

    private static void setField(ClickHouseTableSink sink, String name, Object value) throws Exception {
        Field field = ClickHouseTableSink.class.getDeclaredField(name);
        field.setAccessible(true);
        field.set(sink, value);
    }
}
