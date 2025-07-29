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

package org.apache.geaflow.dsl.connector.file.source;

import static org.apache.geaflow.dsl.connector.file.FileConstants.PREFIX_JAVA_RESOURCE;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.common.config.keys.ConnectorConfigKeys;
import org.apache.geaflow.dsl.common.exception.GeaFlowDSLException;
import org.apache.geaflow.dsl.common.types.TableSchema;
import org.apache.geaflow.dsl.connector.api.Partition;
import org.apache.geaflow.dsl.connector.file.source.FileTableSource.FileSplit;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JarFileReadHandler extends AbstractFileReadHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(JarFileReadHandler.class);

    @Override
    public void init(Configuration tableConf, TableSchema tableSchema, String path) throws IOException {
        super.init(tableConf, tableSchema, path);
        this.path = new Path(path.substring(PREFIX_JAVA_RESOURCE.length()));
    }

    @Override
    public List<Partition> listPartitions(int parallelism) {
        int index = path.toString().lastIndexOf('/');
        String baseDir = path.toString().substring(0, index);
        String fileName = path.toString().substring(index + 1);
        try {
            URL url = getClass().getResource(path.toString());
            InputStream inputStream = url.openStream();
            int lineSplitSize = findLineSplitSize(inputStream);
            inputStream.close();
            if (parallelism == 1 || tableConf.getBoolean(ConnectorConfigKeys.GEAFLOW_DSL_SOURCE_FILE_PARALLEL_MOD)) {
                return Collections.singletonList(new ResourceFileSplit(baseDir, fileName, lineSplitSize));
            } else {
                List<Partition> partitions = splitSingleFile(parallelism, baseDir, fileName, lineSplitSize);
                List<Partition> newPartitions = new ArrayList<>();
                for (int i = 0; i < partitions.size(); i++) {
                    Partition partition = partitions.get(i);
                    FileSplit fileSplit = (FileSplit) partition;
                    newPartitions.add(new ResourceFileSplit(baseDir, fileName, i, lineSplitSize, fileSplit.getSplitStart(), fileSplit.getSplitLength()));
                }
                return newPartitions;
            }
        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
            throw new RuntimeException(e);
        }
    }

    public List<Partition> splitSingleFile(int parallelism, String baseDir, String fileName, int lineSplitSize) throws Exception {
        FileSplit fileSplit = new FileSplit(baseDir, fileName);

        if (this.formatName.equalsIgnoreCase(SourceConstants.PARQUET)) {
            throw new RuntimeException("not support parallel read parquet file in resources");
        } else {
            URL url = getClass().getResource(fileSplit.getPath());
            if (url == null) {
                throw new GeaFlowDSLException("Resource: {} not found", fileSplit.getPath());
            }
            File file = new File(url.getFile());
            long fileLength = file.length();
            long splitSize = fileLength / parallelism;
            if (splitSize == 0) {
                splitSize = 1;
            }
            long startPos = 0;
            long endPos = 0;
            List<Partition> fileSplits = new ArrayList<>();
            for (int i = 0; i < parallelism; i++) {
                startPos = endPos;
                InputStream inputStream = url.openStream();
                endPos = findNextStartPos(startPos + splitSize, fileLength, inputStream);
                inputStream.close();
                fileSplits.add(new ResourceFileSplit(baseDir, fileName, i, lineSplitSize, startPos, endPos - startPos));
                if (endPos >= fileLength) {
                    break;
                }
            }
            return fileSplits;
        }
    }

    public static class ResourceFileSplit extends FileSplit {

        public ResourceFileSplit(String baseDir, String relativePath, int lineSplitSize) {
            super(baseDir, relativePath, lineSplitSize);
        }

        public ResourceFileSplit(String baseDir, String relativePath, int lineSplitSize, long splitStart, long splitLength) {
            super(baseDir, relativePath, lineSplitSize, splitStart, splitLength);
        }

        public ResourceFileSplit(String baseDir, String relativePath, int index, int lineSplitSize, long splitStart, long splitLength) {
            super(baseDir, relativePath, index, lineSplitSize, splitStart, splitLength);
        }

        @Override
        public InputStream openStream(Configuration conf) throws IOException {
            URL url = getClass().getResource(getPath());
            if (url == null) {
                throw new GeaFlowDSLException("Resource: {} not found", getPath());
            }
            InputStream inputStream = url.openStream();
            if (getSplitStart() != -1L) {
                inputStream.skip(getSplitStart());
            }
            return inputStream;
        }

        @Override
        public InputStream openStream(Configuration conf, long inputOffset) throws IOException {
            URL url = getClass().getResource(getPath());
            if (url == null) {
                throw new GeaFlowDSLException("Resource: {} not found", getPath());
            }
            InputStream inputStream = url.openStream();
            if (inputOffset != -1L) {
                if (getSplitStart() != -1L) {
                    inputStream.skip(getSplitStart() + inputOffset);
                } else {
                    inputStream.skip(inputOffset);
                }
            }
            return inputStream;
        }
    }
}
