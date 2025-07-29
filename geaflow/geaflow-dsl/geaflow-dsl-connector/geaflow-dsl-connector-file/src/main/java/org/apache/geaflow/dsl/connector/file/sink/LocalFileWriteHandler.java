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

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.common.config.keys.ConnectorConfigKeys;
import org.apache.geaflow.dsl.common.exception.GeaFlowDSLException;
import org.apache.geaflow.dsl.common.types.StructType;
import org.apache.geaflow.dsl.connector.file.FileConnectorUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LocalFileWriteHandler implements FileWriteHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(LocalFileWriteHandler.class);

    private final String baseDir;

    protected String targetFile;

    private Writer writer;

    public LocalFileWriteHandler(String baseDir) {
        this.baseDir = baseDir;
    }

    @Override
    public void init(Configuration tableConf, StructType schema, int taskIndex) {
        File dirPath = new File(baseDir);
        File filePath = new File(dirPath, FileConnectorUtil.getPartitionFileName(taskIndex));
        try {
            if (!dirPath.exists()) {
                dirPath.mkdirs();
            }
            if (filePath.exists()) {
                if (Configuration.getString(ConnectorConfigKeys.GEAFLOW_DSL_SINK_FILE_COLLISION,
                    (String) ConnectorConfigKeys.GEAFLOW_DSL_SINK_FILE_COLLISION.getDefaultValue(),
                    tableConf.getConfigMap()).equals(ConnectorConfigKeys.GEAFLOW_DSL_SINK_FILE_COLLISION.getDefaultValue())) {
                    String newPath = filePath + "_" + System.currentTimeMillis();
                    targetFile = newPath;
                    this.writer = new BufferedWriter(new FileWriter(newPath));
                    LOGGER.info("path {} exists, create new file path {}", filePath, newPath);
                } else {
                    filePath.delete();
                    targetFile = filePath.getAbsolutePath();
                    this.writer = new BufferedWriter(new FileWriter(filePath));
                    LOGGER.info("path {} exists, replace it {}", filePath);
                }
            } else {
                targetFile = filePath.getAbsolutePath();
                this.writer = new BufferedWriter(new FileWriter(filePath));
                LOGGER.info("create file path {}", filePath);
            }
        } catch (IOException e) {
            throw new GeaFlowDSLException("Error in create file: " + filePath, e);
        }
    }

    @Override
    public void write(String text) throws IOException {
        writer.write(text);
    }

    @Override
    public void flush() throws IOException {
        writer.flush();
    }

    @Override
    public void close() throws IOException {
        if (writer != null) {
            writer.close();
        }
    }
}
