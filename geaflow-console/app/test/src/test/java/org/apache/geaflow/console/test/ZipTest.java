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

package org.apache.geaflow.console.test;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.apache.geaflow.console.common.util.Fmt;
import org.apache.geaflow.console.common.util.ZipUtil;
import org.apache.geaflow.console.common.util.ZipUtil.FileZipEntry;
import org.apache.geaflow.console.common.util.ZipUtil.GeaflowZipEntry;
import org.apache.geaflow.console.common.util.ZipUtil.MemoryZipEntry;
import org.testng.Assert;
import org.testng.annotations.Test;

@Slf4j
public class ZipTest {

    @Test
    void unzip() throws IOException {
        String testPath = "/tmp/geaflow-zipTest-" + UUID.randomUUID() + "/";
        String zipPath = testPath + "test-zip.zip";
        String gqlPath = testPath + "user.gql";
        String confPath = testPath + "user.conf";
        try {
            FileUtils.forceMkdir(new File(testPath));
            HashMap<String, Integer> map = new HashMap<>();
            map.put("aa", 1);
            map.put("b", 3);
            map.put("c", 4);
            GeaflowZipEntry userGql = new MemoryZipEntry("user.gql", code);
            GeaflowZipEntry conf = new MemoryZipEntry("user.conf", JSON.toJSONString(map));

            InputStream inputStream = ZipUtil.buildZipInputStream(Arrays.asList(userGql, conf));
            FileUtils.copyInputStreamToFile(inputStream, new File(zipPath));

            ZipUtil.unzip(new File(zipPath));

            File confFile = new File(confPath);
            String confString = FileUtils.readFileToString(confFile, Charset.defaultCharset());
            JSONObject jsonObject = JSON.parseObject(confString);
            Assert.assertEquals(jsonObject.getInteger("aa").intValue(), 1);

            File gqlFile = new File(gqlPath);
            String gqlString = FileUtils.readFileToString(gqlFile, Charset.defaultCharset());
            Assert.assertEquals(gqlString, code);
        } finally {
            FileUtils.deleteDirectory(new File(testPath));
        }
    }

    @Test
    public void test() throws IOException {
        String path = Fmt.as("/tmp/test-zip-{}.zip", System.currentTimeMillis());
        try {
            HashMap<String, Integer> map = new HashMap<>();
            map.put("aa", 1);
            map.put("b", 3);
            map.put("c", 4);
            GeaflowZipEntry userGql = new MemoryZipEntry("user.gql", code);
            GeaflowZipEntry conf = new MemoryZipEntry("user.conf", JSON.toJSONString(map));

            InputStream inputStream = ZipUtil.buildZipInputStream(Arrays.asList(userGql, conf));
            FileUtils.copyInputStreamToFile(inputStream, new File(path));
            inputStream.close();

            inputStream = Files.newInputStream(Paths.get(path));
            ZipInputStream zipInputStream = new ZipInputStream(inputStream);
            ZipEntry entry;
            while ((entry = zipInputStream.getNextEntry()) != null) {
                ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
                byte[] byte_s = new byte[1024];
                int num;
                while ((num = zipInputStream.read(byte_s, 0, byte_s.length)) > -1) {
                    byteArrayOutputStream.write(byte_s, 0, num);

                }
                String s = byteArrayOutputStream.toString();

                log.info(s);
                zipInputStream.closeEntry();
                byteArrayOutputStream.close();
                if (entry.getName().equals("user.conf")) {
                    JSONObject json = JSON.parseObject(s);
                    Assert.assertEquals(json.get("aa"), 1);
                    Assert.assertEquals(json.get("b"), 3);
                }
                if (entry.getName().equals("user.gql")) {
                    Assert.assertEquals(s, code);
                }
            }
        } finally {
            File file = new File(path);
            file.delete();
        }
    }

    @Test
    public void testMultiFiles() throws IOException, URISyntaxException {
        String path = Fmt.as("/tmp/ziptest/test-zip-{}.zip", System.currentTimeMillis());
        try {
            URL src1 = getClass().getClassLoader().getResource("zip_test.txt");

            String entry1 = "dir/zip_test.txt";

            URL src2 = getClass().getClassLoader().getResource("zip_test2.txt");
            String entry2 = "dir/zip_test2.txt";


            List<GeaflowZipEntry> entries = new ArrayList<>();
            entries.add(new FileZipEntry(entry1, new File(src1.toURI())));
            entries.add(new FileZipEntry(entry2, new File(src2.toURI())));
            InputStream stream = ZipUtil.buildZipInputStream(entries);

            FileUtils.copyInputStreamToFile(stream, new File(path));
        } finally {
            File file = new File(path);
            file.delete();
        }

    }

    String code = "CREATE GRAPH modern (\n" + "\tVertex person (\n" + "\t  id bigint ID,\n" + "\t  name varchar,\n"
        + "\t  age int\n" + "\t),\n" + "\tVertex software (\n" + "\t  id bigint ID,\n" + "\t  name varchar,\n"
        + "\t  lang varchar\n" + "\t),\n" + "\tEdge knows (\n" + "\t  srcId bigint SOURCE ID,\n"
        + "\t  targetId bigint DESTINATION ID,\n" + "\t  weight double\n" + "\t),\n" + "\tEdge created (\n"
        + "\t  srcId bigint SOURCE ID,\n" + "  \ttargetId bigint DESTINATION ID,\n" + "  \tweight double\n" + "\t)\n"
        + ") WITH (\n" + "\tstoreType='rocksdb',\n"
        + "\tgeaflow.dsl.using.vertex.path = 'resource:///data/modern_vertex.txt',\n"
        + "\tgeaflow.dsl.using.edge.path = 'resource:///data/modern_edge.txt'\n" + ");\n" + "\n"
        + "CREATE TABLE tbl_result (\n" + "  a_id bigint,\n" + "  weight double,\n" + "  b_id bigint\n" + ") WITH (\n"
        + "\ttype='file',\n" + "\tgeaflow.dsl.file.path='${target}'\n" + ");\n" + "\n" + "USE GRAPH modern;\n" + "\n"
        + "INSERT INTO tbl_result\n" + "SELECT\n" + "\ta_id,\n" + "\tweight,\n" + "\tb_id\n" + "FROM (\n"
        + "  MATCH (a) -[e:knows]->(b:person where b.id != 1)\n"
        + "  RETURN a.id as a_id, e.weight as weight, b.id as b_id\n" + ")";

}
