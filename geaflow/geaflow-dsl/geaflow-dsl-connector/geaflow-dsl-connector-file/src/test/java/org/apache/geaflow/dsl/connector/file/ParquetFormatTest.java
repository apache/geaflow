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

import static java.lang.Thread.sleep;

import com.google.common.collect.Lists;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.commons.io.FileUtils;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.common.config.keys.ConnectorConfigKeys;
import org.apache.geaflow.common.type.Types;
import org.apache.geaflow.dsl.common.data.Row;
import org.apache.geaflow.dsl.common.types.StructType;
import org.apache.geaflow.dsl.common.types.TableField;
import org.apache.geaflow.dsl.common.types.TableSchema;
import org.apache.geaflow.dsl.connector.file.source.FileTableSource.FileSplit;
import org.apache.geaflow.dsl.connector.file.source.format.ParquetFormat;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.parquet.avro.AvroParquetOutputFormat;
import org.testng.Assert;
import org.testng.annotations.Test;

public class ParquetFormatTest {

    private static final StructType dataSchema = new StructType(
        new TableField("id", Types.INTEGER, false),
        new TableField("name", Types.BINARY_STRING),
        new TableField("price", Types.DOUBLE)
    );

    private static final Schema avroSchema = ParquetFormat.convertToAvroSchema(dataSchema, false);

    @Test
    public void testReadParquet() throws Exception {
        String output = "target/test/parquet/output";

        writeData(output, "1,a1,10", "2,a2,12", "3,a3,15");

        ParquetFormat format = new ParquetFormat();
        Map<String, String> config = new HashMap<>();
        File file = new File(output);
        List<File> parquetFiles = Arrays.stream(file.listFiles())
            .filter(f -> f.getName().endsWith(".parquet"))
            .collect(Collectors.toList());
        Assert.assertEquals(parquetFiles.size(), 1);

        config.put(ConnectorConfigKeys.GEAFLOW_DSL_FILE_PATH.getKey(), parquetFiles.get(0).getAbsolutePath());
        FileSplit fileSplit = new FileSplit(parquetFiles.get(0).getAbsolutePath());
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

    private void writeData(String output, String... lines) throws Exception {
        final org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();

        String inputFile = "target/test/parquet/input.txt";
        FileUtils.writeLines(new File(inputFile), Lists.newArrayList(lines));

        Path inputPath = new Path(inputFile);
        Path outputPath = new Path(output);
        final FileSystem fileSystem = inputPath.getFileSystem(conf);
        fileSystem.delete(outputPath, true);
        final Job job = new Job(conf, "write data to parquet");

        TextInputFormat.addInputPath(job, inputPath);
        job.setInputFormatClass(TextInputFormat.class);

        job.setMapperClass(TestMapper.class);
        job.setNumReduceTasks(0);

        job.setOutputFormatClass(AvroParquetOutputFormat.class);
        AvroParquetOutputFormat.setOutputPath(job, outputPath);
        AvroParquetOutputFormat.setSchema(job, avroSchema);

        job.submit();
        while (!job.isComplete()) {
            sleep(10);
        }
        if (!job.isSuccessful()) {
            throw new RuntimeException("job failed " + job.getJobName());
        }
    }

    public static class TestMapper extends Mapper<LongWritable, Text, Void, GenericRecord> {

        @Override
        public void map(LongWritable key, Text value,
                        Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split(",");
            GenericRecord record = new GenericRecordBuilder(avroSchema)
                .set("id", Integer.parseInt(fields[0]))
                .set("name", fields[1])
                .set("price", Double.parseDouble(fields[2]))
                .build();
            context.write(null, record);
        }
    }
}
