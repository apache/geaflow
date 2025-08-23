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

import java.io.IOException;
import java.util.Objects;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HBaseLocalTestUtils {

    private static final Logger LOGGER = LoggerFactory.getLogger(HBaseLocalTestUtils.class);

    private static Connection connection = null;

    private static final HBaseTestingUtility hBaseTesting;

    static {
        hBaseTesting = new HBaseTestingUtility();
        hBaseTesting.getConfiguration().set("test.hbase.zookeeper.property.clientPort", "2181");
        try {
            hBaseTesting.startMiniCluster();
            if (Objects.isNull(connection)) {
                connection = hBaseTesting.getConnection();
            }
            LOGGER.info("Get connection from HBase local mini cluster");
        } catch (Exception e) {
            throw new RuntimeException("Can not get connection from HBase local mini cluster.");
        }
    }

    public static void closeConnection() {
        try {
            if (Objects.nonNull(connection)) {
                connection.close();
            }
            if (Objects.nonNull(hBaseTesting)) {
                hBaseTesting.shutdownMiniCluster();
            }
        } catch (IOException e) {
            throw new RuntimeException("Can not close resources.");
        }
    }

    private static boolean tableExists(String namespace, String tableName) throws IOException {
        Admin admin = connection.getAdmin();
        boolean exists;
        try {
            exists = admin.tableExists(TableName.valueOf(namespace, tableName));
        } catch (IOException e) {
            throw new RuntimeException("Fail to judge table exists.");
        }
        admin.close();
        return exists;
    }

    public static void createNamespace(String namespace) throws IOException {
        Admin admin = connection.getAdmin();
        NamespaceDescriptor.Builder builder = NamespaceDescriptor.create(namespace);
        try {
            admin.createNamespace(builder.build());
        } catch (IOException e) {
            throw new RuntimeException("Fail to create HBase namespace.");
        }
        LOGGER.info("Create HBase namespace {} success.", namespace);
        admin.close();
    }

    public static void createTable(String namespace, String tableName, String... columnFamilies)
        throws IOException {
        if (columnFamilies.length == 0) {
            throw new RuntimeException("Create a table with at least one column family.");
        }
        if (tableExists(namespace, tableName)) {
            throw new RuntimeException(
                String.format("Namespace %s, table %s already exists.", namespace, tableName));
        }
        Admin admin = connection.getAdmin();
        TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(
            TableName.valueOf(namespace, tableName));

        for (String columnFamily : columnFamilies) {
            ColumnFamilyDescriptorBuilder columnFamilyDescriptorBuilder =
                ColumnFamilyDescriptorBuilder.newBuilder(
                    Bytes.toBytes(columnFamily));
            tableDescriptorBuilder.setColumnFamily(columnFamilyDescriptorBuilder.build());
        }

        try {
            admin.createTable(tableDescriptorBuilder.build());
        } catch (IOException e) {
            throw new RuntimeException("Fail to create HBase table.");
        }
        LOGGER.info("Create HBase table `{}` under namespace `{}` success.", tableName, namespace);
        admin.close();
    }

    public static void putCell(String namespace, String tableName, String rowKey,
                               String columnFamilyName, String qualifier, String value)
        throws IOException {
        Table table = connection.getTable(TableName.valueOf(namespace, tableName));
        Put put = new Put(Bytes.toBytes(rowKey));
        put.addColumn(Bytes.toBytes(columnFamilyName), Bytes.toBytes(qualifier),
            Bytes.toBytes(value));

        try {
            table.put(put);
        } catch (IOException e) {
            throw new RuntimeException("Fail to put one row to HBase.");
        }
        LOGGER.info("Put a record {} to {}-{}-{}-{}-{}.", value, namespace, tableName, rowKey,
            columnFamilyName, qualifier);
        table.close();
    }

    public static byte[] getCell(String namespace, String tableName, String rowKey,
                                 String columnFamilyName, String qualifier) throws IOException {
        Table table = connection.getTable(TableName.valueOf(namespace, tableName));
        Get get = new Get(Bytes.toBytes(rowKey));
        get.addColumn(Bytes.toBytes(columnFamilyName), Bytes.toBytes(qualifier));

        byte[] resultValue;
        try {
            Result result = table.get(get);
            resultValue = result.getValue(Bytes.toBytes(columnFamilyName),
                Bytes.toBytes(qualifier));
        } catch (IOException e) {
            throw new RuntimeException("Fail to get cell from HBase");
        }
        table.close();
        return resultValue;
    }

    public static ResultScanner getScanner(String namespace, String tableName) throws IOException {
        Table table = connection.getTable(TableName.valueOf(namespace, tableName));
        try {
            Scan scan = new Scan();
            return table.getScanner(scan);
        } catch (IOException e) {
            throw new RuntimeException("Fail to scan all records.");
        }
    }
}
