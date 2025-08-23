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

package org.apache.geaflow.dsl.connector.hive;

import java.io.File;
import java.io.IOException;
import org.apache.commons.io.FileUtils;
import org.apache.geaflow.common.utils.FileUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.testng.annotations.BeforeClass;

public class BaseHiveTest {

    public static final int metastorePort = 9083;
    private HiveTestMetaStore hiveTestMetastore;

    private Driver hiveDriver;

    @BeforeClass
    public void setup() throws IOException {
        String hiveLocation = FileUtil.concatPath(FileUtils.getTempDirectoryPath(),
            "hive_" + System.currentTimeMillis());
        File hiveDir = new File(hiveLocation);
        hiveDir.mkdirs();
        HiveConf hiveConf = createHiveConf(new Configuration(), hiveLocation);
        hiveTestMetastore = new HiveTestMetaStore(hiveConf, hiveLocation);
        hiveTestMetastore.start();

        SessionState.start(hiveConf);
        hiveDriver = new Driver(hiveConf);
    }

    public void shutdown() {
        hiveTestMetastore.stop();
    }

    protected void executeHiveSql(String hiveSql) {
        try {
            hiveDriver.run(hiveSql);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private HiveConf createHiveConf(Configuration conf, String hiveLocation) {
        conf.set("hive.metastore.local", "false");
        conf.setInt(ConfVars.METASTORE_SERVER_PORT.varname, metastorePort);
        conf.set(ConfVars.METASTORE_EXECUTE_SET_UGI.varname, "false");
        conf.set(HiveConf.ConfVars.METASTOREURIS.varname, "thrift://localhost:" + metastorePort);
        // conf.set(HiveConf.ConfVars.HIVE_SERVER2_THRIFT_BIND_HOST.varname, bindIP);
        File metastoreDir = new File(hiveLocation, "metastore_db");
        conf.set(HiveConf.ConfVars.METASTORECONNECTURLKEY.varname,
            "jdbc:derby:" + metastoreDir.getPath() + ";create=true");
        File wareHouseDir = new File(hiveLocation, "ware_house");
        wareHouseDir.mkdirs();
        conf.set(HiveConf.ConfVars.METASTOREWAREHOUSE.varname, wareHouseDir.getAbsolutePath());
        conf.set("datanucleus.schema.autoCreateTables", "true");
        conf.set("hive.metastore.schema.verification", "false");
        conf.set("datanucleus.autoCreateSchema", "true");
        conf.set("datanucleus.fixedDatastore", "false");
        conf.set("datanucleus.schema.autoCreateAll", "true");
        conf.set("hive.stats.autogather", "false");

        String scratchDir = FileUtil.concatPath(hiveLocation, "scratch");
        conf.set(ConfVars.SCRATCHDIR.varname, scratchDir);
        return new HiveConf(conf, this.getClass());
    }
}
