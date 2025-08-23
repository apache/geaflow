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

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.regex.Pattern;
import org.apache.commons.lang3.StringUtils;
import org.apache.geaflow.api.context.RuntimeContext;
import org.apache.geaflow.api.window.WindowType;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.common.config.keys.ConnectorConfigKeys;
import org.apache.geaflow.dsl.common.exception.GeaFlowDSLException;
import org.apache.geaflow.dsl.common.types.TableSchema;
import org.apache.geaflow.dsl.connector.api.FetchData;
import org.apache.geaflow.dsl.connector.api.Offset;
import org.apache.geaflow.dsl.connector.api.Partition;
import org.apache.geaflow.dsl.connector.api.TableSource;
import org.apache.geaflow.dsl.connector.api.serde.TableDeserializer;
import org.apache.geaflow.dsl.connector.api.window.FetchWindow;
import org.apache.geaflow.dsl.connector.file.FileConnectorUtil;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileTableSource implements TableSource {

    private static final Logger LOGGER = LoggerFactory.getLogger(FileTableSource.class);

    private String path;

    private Configuration tableConf;

    private TableSchema tableSchema;

    private String nameFilterRegex;

    private transient FileReadHandler fileReadHandler;

    @Override
    public void init(Configuration tableConf, TableSchema tableSchema) {
        this.path = tableConf.getString(ConnectorConfigKeys.GEAFLOW_DSL_FILE_PATH);
        this.tableConf = tableConf;
        this.tableSchema = tableSchema;
        this.nameFilterRegex = tableConf.getString(ConnectorConfigKeys.GEAFLOW_DSL_FILE_NAME_REGEX);
        LOGGER.info("init table source with tableConf: {}", tableConf);
    }

    @Override
    public void open(RuntimeContext context) {
        this.fileReadHandler = FileReadHandlers.from(path);
        try {
            this.fileReadHandler.init(tableConf, tableSchema, path);
        } catch (IOException e) {
            throw new GeaFlowDSLException("Error in open file source", e);
        }
        LOGGER.info("open table source on path: {}", path);
    }

    @Override
    public List<Partition> listPartitions() {
        return listPartitions(1);
    }

    @Override
    public List<Partition> listPartitions(int parallelism) {
        List<Partition> allPartitions = fileReadHandler.listPartitions(parallelism);
        if (StringUtils.isNotEmpty(this.nameFilterRegex)) {
            List<Partition> filterPartitions = new ArrayList<>();
            for (Partition partition : allPartitions) {
                if (!partition.getName().startsWith(".")
                    && Pattern.matches(this.nameFilterRegex, partition.getName())) {
                    filterPartitions.add(partition);
                }
            }
            return filterPartitions;
        } else {
            return allPartitions;
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public <IN> TableDeserializer<IN> getDeserializer(Configuration conf) {
        return fileReadHandler.getDeserializer();
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> FetchData<T> fetch(Partition partition, Optional<Offset> startOffset,
                                  FetchWindow windowInfo) throws IOException {
        int windowSize;
        if (windowInfo.getType() == WindowType.ALL_WINDOW) {
            windowSize = Integer.MAX_VALUE;
        } else if (windowInfo.getType() == WindowType.SIZE_TUMBLING_WINDOW) {
            if (windowInfo.windowSize() > Integer.MAX_VALUE) {
                throw new GeaFlowDSLException("File table source window size is overflow:{}", windowInfo.windowSize());
            }
            windowSize = (int) windowInfo.windowSize();
        } else {
            throw new GeaFlowDSLException("File table source not support window:{}", windowInfo.getType());
        }
        FileOffset offset = startOffset.map(value -> (FileOffset) value).orElseGet(() -> new FileOffset(0L));
        return fileReadHandler.readPartition((FileSplit) partition, offset, windowSize);
    }

    @Override
    public void close() {
        try {
            fileReadHandler.close();
        } catch (IOException e) {
            throw new GeaFlowDSLException("Error in close file read handler", e);
        }
    }

    public static class FileSplit implements Partition {

        private String name;

        private final String baseDir;

        private final String relativePath;

        private long splitStart;

        private long splitLength;

        private int lineSplitSize;

        private int index;

        private int parallel;

        public FileSplit(String baseDir, String relativePath) {
            this.baseDir = baseDir;
            this.relativePath = relativePath;
            this.lineSplitSize = 1;
            this.splitStart = -1L;
            this.splitLength = Long.MAX_VALUE;
            this.name = relativePath;
        }

        public FileSplit(String baseDir, String relativePath, int lineSplitSize) {
            this.baseDir = baseDir;
            this.relativePath = relativePath;
            this.lineSplitSize = lineSplitSize;
            this.splitStart = -1L;
            this.splitLength = Long.MAX_VALUE;
            this.name = relativePath;
        }

        public FileSplit(String baseDir, String relativePath, int lineSplitSize, long splitStart, long splitLength) {
            this.baseDir = baseDir;
            this.relativePath = relativePath;
            this.lineSplitSize = lineSplitSize;
            this.splitStart = splitStart;
            this.splitLength = splitLength;
            this.name = relativePath;
        }

        public FileSplit(String baseDir, String relativePath, int index, int lineSplitSize, long splitStart, long splitLength) {
            this.baseDir = baseDir;
            this.relativePath = relativePath;
            this.lineSplitSize = lineSplitSize;
            this.splitStart = splitStart;
            this.splitLength = splitLength;
            this.name = relativePath + "_" + index;
        }

        public FileSplit(String file) {
            int index = file.lastIndexOf('/');
            if (index == -1) {
                throw new GeaFlowDSLException("Illegal file path: '{}', should be a full path.", file);
            }
            this.baseDir = file.substring(0, index);
            this.relativePath = file.substring(index + 1);
            this.splitStart = -1L;
            this.splitLength = Long.MAX_VALUE;
            this.name = relativePath;
        }

        @Override
        public void setIndex(int index, int parallel) {
            this.index = index;
            this.parallel = parallel;
        }

        @Override
        public String getName() {
            return name;
        }

        public String getPath() {
            if (baseDir.endsWith("/")) {
                return baseDir + relativePath;
            }
            return baseDir + "/" + relativePath;
        }

        public long getSplitStart() {
            return splitStart;
        }

        public long getSplitLength() {
            return splitLength;
        }

        public int getLineSplitSize() {
            return lineSplitSize;
        }

        public int getIndex() {
            return index;
        }

        public int getParallel() {
            return parallel;
        }

        @Override
        public int hashCode() {
            return Objects.hash(baseDir, relativePath);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof FileSplit)) {
                return false;
            }
            FileSplit that = (FileSplit) o;
            return Objects.equals(baseDir, that.baseDir) && Objects.equals(relativePath, that.relativePath)
                && Objects.equals(name, that.name) && Objects.equals(splitStart, that.splitStart)
                && Objects.equals(splitLength, that.splitLength);
        }

        @Override
        public String toString() {
            return "FileSplit(path=" + getPath() + ")";
        }

        public InputStream openStream(Configuration conf) throws IOException {
            FileSystem fs = FileConnectorUtil.getHdfsFileSystem(conf);
            Path path = new Path(baseDir, relativePath);
            FSDataInputStream inputStream = fs.open(path);
            if (this.splitStart != -1L) {
                inputStream.seek(this.splitStart);
            }
            return inputStream;
        }

        public InputStream openStream(Configuration conf, long inputOffset) throws IOException {
            FileSystem fs = FileConnectorUtil.getHdfsFileSystem(conf);
            Path path = new Path(baseDir, relativePath);
            FSDataInputStream inputStream = fs.open(path);
            if (this.splitStart != -1L) {
                inputStream.seek(this.splitStart + inputOffset);
            }
            return inputStream;
        }

    }

    public static class FileOffset implements Offset {

        private final long offset;

        public FileOffset(long offset) {
            this.offset = offset;
        }

        @Override
        public String humanReadable() {
            return String.valueOf(offset);
        }

        @Override
        public long getOffset() {
            return offset;
        }

        @Override
        public boolean isTimestamp() {
            return false;
        }
    }
}
