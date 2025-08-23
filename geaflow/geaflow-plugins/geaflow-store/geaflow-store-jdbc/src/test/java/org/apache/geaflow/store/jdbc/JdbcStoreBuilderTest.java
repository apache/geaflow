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

package org.apache.geaflow.store.jdbc;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import org.apache.commons.io.FileUtils;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.state.DataModel;
import org.apache.geaflow.state.StoreType;
import org.apache.geaflow.state.serializer.DefaultKVSerializer;
import org.apache.geaflow.store.IStoreBuilder;
import org.apache.geaflow.store.api.StoreBuilderFactory;
import org.apache.geaflow.store.api.key.IKVStore;
import org.apache.geaflow.store.context.StoreContext;
import org.testng.Assert;
import org.testng.annotations.Test;

public class JdbcStoreBuilderTest {

    private void prepareSqlite() throws Exception {
        String createSqliteTable = "CREATE TABLE IF NOT EXISTS `store_test`(\n"
            + "   `id` bigint UNSIGNED AUTO_INCREMENT,\n"
            + "   `pk` VARCHAR(256) NOT NULL,\n"
            + "   `value` LONGBLOB,\n"
            + "   PRIMARY KEY ( `id`)\n"
            + ")";

        Class.forName("org.sqlite.JDBC");
        Connection c = DriverManager.getConnection("jdbc:sqlite:/tmp/test.db");
        Statement stmt = c.createStatement();
        stmt.execute(createSqliteTable);
        stmt.close();
        c.close();
    }

    @Test
    public void testSqliteKV() throws Exception {
        prepareSqlite();
        Configuration configuration = new Configuration();
        configuration.put(JdbcConfigKeys.JDBC_DRIVER_CLASS.getKey(), "org.sqlite.JDBC");
        configuration.put(JdbcConfigKeys.JDBC_URL, "jdbc:sqlite:/tmp/test.db");
        configuration.put(JdbcConfigKeys.JDBC_INSERT_FORMAT, "INSERT INTO %s(%s,%s) VALUES (?, %s)");
        configuration.put(JdbcConfigKeys.JDBC_UPDATE_FORMAT, "UPDATE %s SET %s=? WHERE %s='%s'");

        IStoreBuilder builder = StoreBuilderFactory.build(StoreType.JDBC.name());
        IKVStore<String, String> kvStore =
            (IKVStore<String, String>) builder.getStore(DataModel.KV, configuration);
        StoreContext storeContext = new StoreContext("store_test").withConfig(configuration);
        storeContext.withKeySerializer(new DefaultKVSerializer<>(String.class, String.class));
        Assert.assertEquals(kvStore.getClass().getSimpleName(), JdbcKVStore.class.getSimpleName());

        kvStore.init(storeContext);

        innerTestKV(kvStore);
        FileUtils.deleteQuietly(new File("/tmp/test.db"));
        kvStore.close();
    }

    private static void innerTestKV(IKVStore<String, String> kvStore) {
        String key = "key1";
        kvStore.put(key, "foo");
        Assert.assertEquals(kvStore.get(key), "foo");

        Assert.assertEquals(kvStore.get(key), "foo");

        kvStore.remove(key);
        Assert.assertNull(kvStore.get(key));

        for (int i = 0; i < 5; i++) {
            key = "key" + i;
            kvStore.put(key, "foo");
            Assert.assertEquals(kvStore.get(key), "foo");
        }
    }
}
