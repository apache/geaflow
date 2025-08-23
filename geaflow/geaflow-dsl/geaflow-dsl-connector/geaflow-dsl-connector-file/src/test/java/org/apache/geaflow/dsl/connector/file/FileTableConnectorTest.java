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

package org.apache.geaflow.dsl.connector.file;

import com.alibaba.fastjson.JSON;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.common.config.keys.ConnectorConfigKeys;
import org.apache.geaflow.common.type.primitive.StringType;
import org.apache.geaflow.dsl.common.types.StructType;
import org.apache.geaflow.dsl.common.types.TableField;
import org.apache.geaflow.dsl.common.types.TableSchema;
import org.apache.geaflow.dsl.connector.api.FetchData;
import org.apache.geaflow.dsl.connector.api.Partition;
import org.apache.geaflow.dsl.connector.api.function.OffsetStore.ConsoleOffset;
import org.apache.geaflow.dsl.connector.file.sink.FileTableSink;
import org.apache.geaflow.dsl.connector.file.sink.HdfsFileWriteHandler;
import org.apache.geaflow.dsl.connector.file.source.DfsFileReadHandler;
import org.apache.geaflow.dsl.connector.file.source.FileTableSource.FileOffset;
import org.apache.geaflow.dsl.connector.file.source.FileTableSource.FileSplit;
import org.apache.geaflow.file.FileConfigKeys;
import org.testng.Assert;
import org.testng.annotations.Test;

public class FileTableConnectorTest {

    @Test
    public void testLocalFileResource() throws IOException {
        DfsFileReadHandler resource = new DfsFileReadHandler();
        resource.init(new Configuration(), new TableSchema(), "file:///data");

        try {
            resource.readPartition(new FileSplit("/data", "test"),
                new FileOffset(0L), 0);
        } catch (Exception e) {
            Assert.assertEquals(e.getMessage(), "File /data/test does not exist");
        }
        resource.close();
        FileSplit fileSplit = new FileSplit("test_baseDir", "test_relativePath");
        Assert.assertEquals(fileSplit.getName(), "test_relativePath");
        Assert.assertEquals(fileSplit.getPath(), "test_baseDir/test_relativePath");
        FileSplit fileSplit2 = new FileSplit("test_baseDir/test_relativePath");
        Assert.assertEquals(fileSplit.hashCode(), fileSplit2.hashCode());
        Assert.assertEquals(fileSplit, fileSplit2);
        Assert.assertEquals(fileSplit, fileSplit);
        Assert.assertNotEquals(fileSplit, null);
        Assert.assertEquals(fileSplit.toString(), fileSplit2.toString());

        FileOffset fileOffset = new FileOffset(1000L);
        Assert.assertEquals(fileOffset.getOffset(), 1000L);
        Assert.assertEquals(fileOffset.humanReadable(), "1000");
    }

    @Test
    public void testFileTableSink() {
        FileTableSink tableSink = new FileTableSink();
        Configuration conf = new Configuration();
        conf.put(ConnectorConfigKeys.GEAFLOW_DSL_FILE_PATH, "test");
        tableSink.init(conf, new StructType(new TableField("test", StringType.INSTANCE, true)));
        tableSink.close();
    }

    @Test
    public void testFileHandlerReandAndWrite() throws IOException {
        String testDir = "/tmp/testDirForHdfsFileWriteHandlerTest";
        FileUtils.deleteDirectory(new File(testDir));
        HdfsFileWriteHandler handler = new HdfsFileWriteHandler(testDir);
        Configuration testConf = new Configuration();
        testConf.put(FileConfigKeys.JSON_CONFIG.getKey(), "{\"fs.defaultFS\":\"local\"}");
        handler.init(testConf, StructType.singleValue(StringType.INSTANCE, true), 0);
        handler.write("test");
        handler.flush();
        handler.close();
        FileInputStream inputStream = new FileInputStream(testDir + "/partition_0");
        BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
        String line = reader.readLine();
        Assert.assertEquals(line, "test");


        DfsFileReadHandler readHandler = new DfsFileReadHandler();
        readHandler.init(testConf, new TableSchema(), testDir);
        List<Partition> partitions = readHandler.listPartitions(1);
        Assert.assertEquals(partitions.size(), 1);
        FetchData<String> fetchData =
            readHandler.readPartition((FileSplit) partitions.get(0), new FileOffset(0L), 10);
        Assert.assertEquals(fetchData.getDataSize(), 1);
        Assert.assertEquals(fetchData.getDataIterator().next(), "test");
        handler.close();
    }

    @Test
    public void testConsoleOffset() {
        FileOffset test = new FileOffset(111L);
        Map<String, String> kvMap = JSON.parseObject(new ConsoleOffset(test).toJson(), Map.class);
        Assert.assertEquals(kvMap.get("offset"), "111");
        Assert.assertEquals(kvMap.get("type"), "NON_TIMESTAMP");
        Assert.assertTrue(Long.parseLong(kvMap.get("writeTime")) > 0L);
    }
}
