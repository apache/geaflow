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

package org.apache.geaflow.dsl.connector.file.sink;

import java.io.IOException;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.dsl.common.exception.GeaFlowDSLException;
import org.apache.geaflow.dsl.common.types.StructType;
import org.apache.geaflow.dsl.connector.file.FileConnectorUtil;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HdfsFileWriteHandler implements FileWriteHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(HdfsFileWriteHandler.class);

    protected Configuration conf;
    protected StructType schema;
    protected int taskIndex;
    protected final String baseDir;

    protected transient FileSystem fileSystem;
    protected transient FSDataOutputStream writer;

    public HdfsFileWriteHandler(String baseDir) {
        this.baseDir = baseDir;
    }

    @Override
    public void init(Configuration tableConf, StructType schema, int taskIndex) {
        this.conf = tableConf;
        this.schema = schema;
        this.taskIndex = taskIndex;
        this.fileSystem = FileConnectorUtil.getHdfsFileSystem(conf);

        Path dirPath = new Path(baseDir);
        Path filePath = new Path(dirPath, FileConnectorUtil.getPartitionFileName(taskIndex));
        filePath = fileSystem.makeQualified(filePath);
        try {
            if (!fileSystem.exists(new Path(baseDir))) {
                fileSystem.mkdirs(new Path(baseDir));
                LOGGER.info("mkdirs {}", baseDir);
            }
            if (fileSystem.exists(filePath)) {
                String newPath = filePath + "_" + System.currentTimeMillis();
                this.writer = fileSystem.create(new Path(newPath));
                LOGGER.info("path {} exists, create new file path {}", filePath, newPath);
            } else {
                this.writer = fileSystem.create(filePath);
                LOGGER.info("create file path {}", filePath);
            }
        } catch (IOException e) {
            throw new GeaFlowDSLException("Error in create file: " + filePath, e);
        }
    }

    @Override
    public void write(String text) throws IOException {
        this.writer.write(text.getBytes());
    }

    @Override
    public void flush() throws IOException {
        this.writer.flush();
        this.writer.hflush();
    }

    @Override
    public void close() throws IOException {
        if (this.writer != null) {
            flush();
            this.writer.close();
        }
    }
}
