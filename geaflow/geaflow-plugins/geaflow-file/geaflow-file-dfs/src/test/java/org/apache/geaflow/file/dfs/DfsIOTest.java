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

package org.apache.geaflow.file.dfs;

import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.common.utils.GsonUtil;
import org.apache.geaflow.file.FileConfigKeys;
import org.apache.geaflow.file.FileInfo;
import org.apache.geaflow.file.IPersistentIO;
import org.apache.geaflow.file.PersistentIOBuilder;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class DfsIOTest {

    private MiniDFSCluster hdfsCluster;
    private String hdfsURI;

    @BeforeClass
    public void createHDFS() throws Exception {
        org.apache.hadoop.conf.Configuration hdConf = new org.apache.hadoop.conf.Configuration();
        File baseDir = new File("./target/hdfs/hdfsTest").getAbsoluteFile();
        FileUtils.deleteQuietly(baseDir);
        hdConf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, baseDir.getAbsolutePath());
        MiniDFSCluster.Builder builder = new MiniDFSCluster.Builder(hdConf);
        hdfsCluster = builder.build();

        hdfsURI = "hdfs://"
            + hdfsCluster.getURI().getHost()
            + ":"
            + hdfsCluster.getNameNodePort()
            + "/";

        Path hdPath = new Path("/test");
        FileSystem hdfs = hdPath.getFileSystem(hdConf);
        FSDataOutputStream stream = hdfs.create(hdPath);
        for (int i = 0; i < 10; i++) {
            stream.write("Hello HDFS\n".getBytes());
        }
        stream.close();
    }

    @AfterClass
    public void tearUp() throws Exception {
        this.hdfsCluster.shutdown(true);
    }

    private void test(Configuration configuration) throws Exception {
        FileUtils.touch(new File("/tmp/README"));

        IPersistentIO persistentIO = PersistentIOBuilder.build(configuration);
        String myName = "myName" + System.currentTimeMillis();
        persistentIO.delete(new Path("/geaflow/chk/" + myName + "2"), true);

        for (int i = 0; i < 101; i++) {
            persistentIO.copyFromLocalFile(new Path("/tmp/README"),
                new Path("/geaflow/chk/" + myName + "/datas/README" + i));
        }
        persistentIO.copyFromLocalFile(new Path("/tmp/README"), new Path(
            "/geaflow/chk/" + myName + "/0/README"));
        persistentIO.copyFromLocalFile(new Path("/tmp/README"), new Path(
            "/geaflow/chk/" + myName + "/1/README"));

        persistentIO.renameFile(new Path("/geaflow/chk/" + myName + "/"), new Path(
            "/geaflow/chk/" + myName + "2"));
        List<String> list = persistentIO.listFileName(new Path("/geaflow/chk/" + myName + "2"));
        Assert.assertEquals(list.size(), 3);

        FileInfo[] res = persistentIO.listFileInfo(new Path("/geaflow/chk/" + myName + "2/datas"));
        Assert.assertEquals(res.length, 101);

        persistentIO.renameFile(new Path("/geaflow/chk/" + myName + "2/datas/README46"),
            new Path("/geaflow/chk/" + myName + "2/datas/MYREADME46"));
        Assert.assertTrue(persistentIO.exists(new Path(
            "/geaflow/chk/" + myName + "2/datas/MYREADME46")));
        Assert.assertFalse(persistentIO.exists(new Path(
            "/geaflow/chk/" + myName + "2/datas/README46")));

        persistentIO.delete(new Path("/geaflow/chk/" + myName + "2"), true);
    }

    @Test
    public void testHdfs() throws Exception {
        Configuration configuration = new Configuration();
        configuration.put(FileConfigKeys.PERSISTENT_TYPE, "DFS");

        Map<String, String> config = new HashMap<>();
        config.put("fs.defaultFS", hdfsURI);
        configuration.put(FileConfigKeys.JSON_CONFIG, GsonUtil.toJson(config));
        test(configuration);
    }
}
