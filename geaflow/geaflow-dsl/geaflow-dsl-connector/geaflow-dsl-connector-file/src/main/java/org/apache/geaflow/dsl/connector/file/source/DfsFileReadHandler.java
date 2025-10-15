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
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import org.apache.avro.generic.GenericData;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.common.config.keys.ConnectorConfigKeys;
import org.apache.geaflow.common.exception.GeaflowRuntimeException;
import org.apache.geaflow.dsl.common.types.TableSchema;
import org.apache.geaflow.dsl.connector.api.Partition;
import org.apache.geaflow.dsl.connector.file.FileConnectorUtil;
import org.apache.geaflow.dsl.connector.file.source.FileTableSource.FileSplit;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.task.JobContextImpl;
import org.apache.parquet.avro.AvroParquetInputFormat;
import org.apache.parquet.hadoop.ParquetInputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DfsFileReadHandler extends AbstractFileReadHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(DfsFileReadHandler.class);

    protected Configuration tableConf;

    @Override
    public void init(Configuration tableConf, TableSchema tableSchema, String path) throws IOException {
        super.init(tableConf, tableSchema, path);
        org.apache.hadoop.conf.Configuration hadoopConf = FileConnectorUtil.toHadoopConf(tableConf);
        this.path = this.path.getFileSystem(hadoopConf).makeQualified(new Path(path));
        this.fileFormats = new HashMap<>();
        this.tableConf = tableConf;
        LOGGER.info("init hdfs file system. path: {}", path);
    }

    @Override
    public List<Partition> listPartitions(int parallelism) {
        try {
            int lineSplitSize = findLineSplitSize();
            if (!tableConf.getBoolean(ConnectorConfigKeys.GEAFLOW_DSL_SOURCE_FILE_PARALLEL_MOD)) {
                FileSystem fileSystem = FileConnectorUtil.getHdfsFileSystem(tableConf);
                if (fileSystem.isDirectory(path)) {
                    RemoteIterator<LocatedFileStatus> files = fileSystem.listFiles(path, true);
                    List<Partition> partitions = new ArrayList<>();
                    while (files.hasNext()) {
                        LocatedFileStatus f = files.next();
                        String relativePath = f.getPath().getName();
                        FileSplit partition = new FileSplit(path.toString(), relativePath, lineSplitSize);
                        partitions.add(partition);
                    }
                    return partitions;
                } else {
                    return splitSingleFile(parallelism, lineSplitSize);
                }
            } else {
                return splitSingleFile(parallelism, lineSplitSize);
            }
        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
            throw new GeaflowRuntimeException("Cannot get partitions with path: " + path, e);
        }
    }

    public int findLineSplitSize() throws Exception {
        if (tableConf.getInteger(ConnectorConfigKeys.GEAFLOW_DSL_FILE_LINE_SPLIT_SIZE) > 0) {
            return tableConf.getInteger(ConnectorConfigKeys.GEAFLOW_DSL_FILE_LINE_SPLIT_SIZE);
        } else if (this.formatName.equalsIgnoreCase(SourceConstants.PARQUET)) {
            return 1;
        } else {
            FileSystem fs = FileConnectorUtil.getHdfsFileSystem(tableConf);
            Path currentPath = path;
            if (fs.isDirectory(path)) {
                RemoteIterator<LocatedFileStatus> files = fs.listFiles(path, true);
                if (!files.hasNext()) {
                    return 1;
                }
                currentPath = files.next().getPath();
            }
            FSDataInputStream inputStream = fs.open(currentPath);
            int lineSplitSize = findLineSplitSize(inputStream);
            inputStream.close();
            return lineSplitSize;
        }
    }

    public List<Partition> splitSingleFile(int parallelism, int lineSplitSize) throws Exception {
        String relativePath = path.getName();
        String directory = path.getParent().toUri().getPath();
        if (parallelism == 1 || tableConf.getBoolean(ConnectorConfigKeys.GEAFLOW_DSL_SOURCE_FILE_PARALLEL_MOD)) {
            FileSplit partition = new FileSplit(directory, relativePath);
            return Collections.singletonList(partition);
        } else {
            if (this.formatName.equalsIgnoreCase(SourceConstants.PARQUET)) {
                ParquetInputFormat<GenericData.Record> parquetInputFormat = new AvroParquetInputFormat<>();
                List<InputSplit> inputSplits = parquetInputFormat.getSplits(new JobContextImpl(FileConnectorUtil.toHadoopConf(tableConf), new JobID()));
                List<Partition> fileSplits = new ArrayList<>();
                inputSplits.stream().forEach(split -> fileSplits.add(new FileSplit(directory, relativePath, lineSplitSize,
                    ((org.apache.hadoop.mapreduce.lib.input.FileSplit) split).getStart(),
                    ((org.apache.hadoop.mapreduce.lib.input.FileSplit) split).getLength())));
                return fileSplits;
            } else {
                FileSystem fs = FileConnectorUtil.getHdfsFileSystem(tableConf);
                long fileLength = fs.getFileStatus(path).getLen();
                long splitSize = fileLength / parallelism;
                if (splitSize == 0) {
                    splitSize = 1;
                }
                long startPos = 0;
                long endPos = 0;
                List<Partition> fileSplits = new ArrayList<>();
                for (int i = 0; i < parallelism; i++) {
                    startPos = endPos;
                    FSDataInputStream inputStream = fs.open(path);
                    endPos = findNextStartPos(startPos + splitSize, fileLength, inputStream);
                    inputStream.close();
                    fileSplits.add(new FileSplit(directory, relativePath, i, lineSplitSize, startPos, endPos - startPos));
                    if (endPos >= fileLength) {
                        break;
                    }
                }
                return fileSplits;
            }
        }
    }
}
