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

package org.apache.geaflow.dsl.connector.hbase;

import static org.apache.geaflow.dsl.connector.hbase.HBaseConstants.DEFAULT_COLUMN_FAMILY;

import com.google.common.collect.Lists;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.apache.geaflow.api.context.RuntimeContext;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.common.type.IType;
import org.apache.geaflow.common.type.Types;
import org.apache.geaflow.dsl.common.data.Row;
import org.apache.geaflow.dsl.common.exception.GeaFlowDSLException;
import org.apache.geaflow.dsl.common.types.StructType;
import org.apache.geaflow.dsl.connector.api.TableSink;
import org.apache.geaflow.utils.JsonUtils;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.BufferedMutatorParams;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HBaseTableSink implements TableSink {

    private static final Logger LOGGER = LoggerFactory.getLogger(HBaseTableSink.class);

    private StructType schema;

    private String zookeeperQuorum;

    private String namespace;

    private String tableName;

    private Set<String> rowKeyColumns;

    private String separator;

    private Map<String, String> familyNamesMap;

    private int bufferSize;

    private Connection connection;

    private BufferedMutator mutator;

    @Override
    public void init(Configuration tableConf, StructType schema) {
        LOGGER.info("Prepare with config: {}, \n schema: {}", tableConf, schema);
        this.schema = schema;

        this.zookeeperQuorum =
            tableConf.getString(HBaseConfigKeys.GEAFLOW_DSL_HBASE_ZOOKEEPER_QUORUM);
        this.tableName = tableConf.getString(HBaseConfigKeys.GEAFLOW_DSL_HBASE_TABLE_NAME);
        this.namespace = tableConf.getString(HBaseConfigKeys.GEAFLOW_DSL_HBASE_NAME_SPACE);
        String rowKeys = tableConf.getString(HBaseConfigKeys.GEAFLOW_DSL_HBASE_ROWKEY_COLUMNS);
        this.rowKeyColumns = new HashSet<>(Arrays.asList(rowKeys.split("\\s*,\\s*")));
        this.separator = tableConf.getString(HBaseConfigKeys.GEAFLOW_DSL_HBASE_ROWKEY_SEPARATOR);
        String familyNameMapping = tableConf.getString(
            HBaseConfigKeys.GEAFLOW_DSL_HBASE_FAMILY_NAME);
        this.familyNamesMap = JsonUtils.parseJson2map(familyNameMapping);
        this.bufferSize = tableConf.getInteger(HBaseConfigKeys.GEAFLOW_DSL_HBASE_BUFFER_SIZE);
    }

    @Override
    public void open(RuntimeContext context) {
        org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();
        conf.set("hbase.zookeeper.quorum", zookeeperQuorum);
        try {
            connection = ConnectionFactory.createConnection(conf);
            BufferedMutatorParams bufferedMutatorParams = new BufferedMutatorParams(
                TableName.valueOf(namespace, tableName))
                .writeBufferSize(bufferSize);
            mutator = connection.getBufferedMutator(bufferedMutatorParams);
        } catch (IOException e) {
            throw new GeaFlowDSLException("Can not get connection from hbase");
        }
    }

    @Override
    public void write(Row row) throws IOException {
        Put put = row2Put(row);
        mutator.mutate(put);
    }

    @Override
    public void finish() throws IOException {
        mutator.flush();
    }

    @Override
    public void close() {
        try {
            if (Objects.nonNull(this.mutator)) {
                mutator.close();
            }
            if (Objects.nonNull(this.connection)) {
                connection.close();
            }
        } catch (IOException e) {
            throw new GeaFlowDSLException("Fail to close resources.");
        }
    }

    private Put row2Put(Row row) {
        byte[] rowKey = buildRowKey(row);
        Put put = new Put(rowKey);
        List<String> fieldNames = this.schema.getFieldNames();
        IType<?>[] types = this.schema.getTypes();
        for (int i = 0; i < fieldNames.size(); i++) {
            if (rowKeyColumns.contains(fieldNames.get(i))) {
                continue;
            }
            String fieldName = fieldNames.get(i);
            String familyName = familyNamesMap.getOrDefault(fieldName, DEFAULT_COLUMN_FAMILY);
            byte[] values = convertColumnToBytes(row, types, i);
            put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes(fieldName), values);
        }
        return put;
    }

    private byte[] buildRowKey(Row row) {
        List<String> fieldNames = this.schema.getFieldNames();
        IType<?>[] types = this.schema.getTypes();
        List<String> rowKeyValues = Lists.newArrayList();
        for (int i = 0; i < fieldNames.size(); i++) {
            if (rowKeyColumns.contains(fieldNames.get(i))) {
                rowKeyValues.add(row.getField(i, types[i]).toString());
            }
        }
        return Bytes.toBytes(String.join(separator, rowKeyValues));
    }

    private byte[] convertColumnToBytes(Row row, IType<?>[] types, int idx) {
        Object field = row.getField(idx, types[idx]);
        if (Objects.isNull(field)) {
            return null;
        }
        String typeName = types[idx].getName();
        switch (typeName) {
            case Types.TYPE_NAME_BYTE:
                return Bytes.toBytes((Byte) field);
            case Types.TYPE_NAME_SHORT:
                return Bytes.toBytes((Short) field);
            case Types.TYPE_NAME_INTEGER:
                return Bytes.toBytes((Integer) field);
            case Types.TYPE_NAME_LONG:
                return Bytes.toBytes((Long) field);
            case Types.TYPE_NAME_BOOLEAN:
                return Bytes.toBytes((Boolean) field);
            case Types.TYPE_NAME_FLOAT:
                return Bytes.toBytes((Float) field);
            case Types.TYPE_NAME_DOUBLE:
                return Bytes.toBytes((Double) field);
            case Types.TYPE_NAME_STRING:
                return field.toString().getBytes(StandardCharsets.UTF_8);
            case Types.TYPE_NAME_BINARY_STRING:
                return field.toString().getBytes();
            default:
                throw new GeaFlowDSLException(String.format("Type: %s is not supported.",
                    typeName));
        }
    }
}
