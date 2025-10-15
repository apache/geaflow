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

package org.apache.geaflow.dsl.connector.hive.adapter;

import java.lang.reflect.Method;
import java.util.Properties;
import org.apache.geaflow.dsl.common.exception.GeaFlowDSLException;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.RetryingMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.Table;

public class Hive23Adapter implements HiveVersionAdapter {

    private final String version;
    private static final String METHOD_NAME_GET_PROXY = "getProxy";
    private static final String METHOD_NAME_GET_TABLE_META_DATA = "getTableMetadata";

    public Hive23Adapter(String version) {
        this.version = version;
    }

    @Override
    public String version() {
        return version;
    }

    @Override
    public IMetaStoreClient createMetaSoreClient(HiveConf hiveConf) {
        try {
            Method method = RetryingMetaStoreClient.class
                .getMethod(METHOD_NAME_GET_PROXY, HiveConf.class, Boolean.TYPE);
            return (IMetaStoreClient) method.invoke(null, hiveConf, true);
        } catch (Exception ex) {
            throw new RuntimeException("Failed to create Hive Metastore client", ex);
        }
    }

    @Override
    public Properties getTableMetadata(Table table) {
        try {
            Class metaStoreUtilsClass = Class.forName("org.apache.hadoop.hive.metastore.MetaStoreUtils");
            Method method =
                metaStoreUtilsClass.getMethod(METHOD_NAME_GET_TABLE_META_DATA, Table.class);
            return (Properties) method.invoke(null, table);
        } catch (Exception e) {
            throw new GeaFlowDSLException(e);
        }
    }
}
