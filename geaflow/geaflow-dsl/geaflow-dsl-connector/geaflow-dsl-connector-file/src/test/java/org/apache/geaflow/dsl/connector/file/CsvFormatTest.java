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

import com.google.common.collect.Lists;
import java.io.File;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.common.config.keys.ConnectorConfigKeys;
import org.apache.geaflow.common.type.Types;
import org.apache.geaflow.dsl.common.data.Row;
import org.apache.geaflow.dsl.common.types.StructType;
import org.apache.geaflow.dsl.common.types.TableField;
import org.apache.geaflow.dsl.common.types.TableSchema;
import org.apache.geaflow.dsl.connector.file.source.FileTableSource.FileSplit;
import org.apache.geaflow.dsl.connector.file.source.format.CsvFormat;
import org.testng.Assert;
import org.testng.annotations.Test;

public class CsvFormatTest {

    @Test
    public void testReadSkipHeader() throws Exception {
        String output = "target/test/csv/output";
        writeData(output, "id,name,price", "1,a1,10", "2,a2,12", "3,a3,15");

        CsvFormat format = new CsvFormat();
        Map<String, String> config = new HashMap<>();
        File outputFile = new File(output);
        config.put(ConnectorConfigKeys.GEAFLOW_DSL_FILE_PATH.getKey(), outputFile.getAbsolutePath());
        config.put(ConnectorConfigKeys.GEAFLOW_DSL_SKIP_HEADER.getKey(), "true");

        FileSplit fileSplit = new FileSplit(outputFile.getAbsolutePath());
        StructType dataSchema = new StructType(
            new TableField("price", Types.DOUBLE),
            new TableField("name", Types.BINARY_STRING)
        );
        format.init(new Configuration(config), new TableSchema(dataSchema), fileSplit);
        Iterator<Row> iterator = format.batchRead();
        StringBuilder result = new StringBuilder();
        while (iterator.hasNext()) {
            Row row = iterator.next();
            if (result.length() > 0) {
                result.append("\n");
            }
            result.append(row.toString());
        }
        Assert.assertEquals(result.toString(),
            "[10.0, a1]\n"
                + "[12.0, a2]\n"
                + "[15.0, a3]");
    }

    @Test
    public void testReadNoHeader() throws Exception {
        String output = "target/test/csv/output";
        writeData(output, "1,a1,10", "2,a2,12", "3,a3,15");

        CsvFormat format = new CsvFormat();
        Map<String, String> config = new HashMap<>();
        File outputFile = new File(output);
        config.put(ConnectorConfigKeys.GEAFLOW_DSL_FILE_PATH.getKey(), outputFile.getAbsolutePath());
        config.put(ConnectorConfigKeys.GEAFLOW_DSL_SKIP_HEADER.getKey(), "false");

        FileSplit fileSplit = new FileSplit(outputFile.getAbsolutePath());
        StructType dataSchema = new StructType(
            new TableField("id", Types.INTEGER, false),
            new TableField("name", Types.BINARY_STRING),
            new TableField("price", Types.DOUBLE)
        );
        format.init(new Configuration(config), new TableSchema(dataSchema), fileSplit);
        Iterator<Row> iterator = format.batchRead();
        StringBuilder result = new StringBuilder();
        while (iterator.hasNext()) {
            Row row = iterator.next();
            if (result.length() > 0) {
                result.append("\n");
            }
            result.append(row.toString());
        }
        Assert.assertEquals(result.toString(),
            "[1, a1, 10.0]\n"
                + "[2, a2, 12.0]\n"
                + "[3, a3, 15.0]");
    }

    private void writeData(String outputFile, String... lines) throws Exception {
        FileUtils.writeLines(new File(outputFile), Lists.newArrayList(lines));
    }
}
