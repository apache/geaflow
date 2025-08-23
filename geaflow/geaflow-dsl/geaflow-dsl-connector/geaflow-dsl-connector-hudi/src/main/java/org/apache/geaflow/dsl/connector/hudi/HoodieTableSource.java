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

package org.apache.geaflow.dsl.connector.hudi;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import org.apache.geaflow.api.context.RuntimeContext;
import org.apache.geaflow.api.window.WindowType;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.common.config.keys.ConnectorConfigKeys;
import org.apache.geaflow.dsl.common.data.Row;
import org.apache.geaflow.dsl.common.exception.GeaFlowDSLException;
import org.apache.geaflow.dsl.common.types.TableSchema;
import org.apache.geaflow.dsl.connector.api.FetchData;
import org.apache.geaflow.dsl.connector.api.Offset;
import org.apache.geaflow.dsl.connector.api.Partition;
import org.apache.geaflow.dsl.connector.api.TableSource;
import org.apache.geaflow.dsl.connector.api.serde.TableDeserializer;
import org.apache.geaflow.dsl.connector.api.window.FetchWindow;
import org.apache.geaflow.dsl.connector.file.FileConnectorUtil;
import org.apache.geaflow.dsl.connector.file.source.FileTableSource.FileOffset;
import org.apache.geaflow.dsl.connector.file.source.FileTableSource.FileSplit;
import org.apache.geaflow.dsl.connector.file.source.format.ParquetFormat;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hudi.BaseHoodieTableFileIndex.PartitionPath;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HoodieTableSource implements TableSource {

    private static final Logger LOGGER = LoggerFactory.getLogger(HoodieTableSource.class);

    private Configuration tableConf;

    private TableSchema tableSchema;

    private GeaFlowHoodieTableFileIndex fileIndex;

    private Path basePath;

    @Override
    public void init(Configuration tableConf, TableSchema tableSchema) {
        this.tableConf = tableConf;
        this.tableSchema = tableSchema;
    }

    @Override
    public void open(RuntimeContext context) {
        this.fileIndex = GeaFlowHoodieTableFileIndex.create(context, tableConf);
        this.basePath = new Path(tableConf.getString(ConnectorConfigKeys.GEAFLOW_DSL_FILE_PATH));
        FileSystem fileSystem = FileConnectorUtil.getHdfsFileSystem(tableConf);
        this.basePath = fileSystem.makeQualified(basePath);
        LOGGER.info("open hudi table source: {}", basePath);
    }

    @Override
    public List<Partition> listPartitions() {
        List<PartitionPath> paths = fileIndex.getAllQueryPartitionPaths();
        List<Partition> fileSplits = new ArrayList<>();
        for (PartitionPath path : paths) {
            fileSplits.add(new FileSplit(basePath.toString(), path.getPath()));
        }
        return fileSplits;
    }

    @Override
    public List<Partition> listPartitions(int parallelism) {
        return listPartitions();
    }

    @Override
    public <IN> TableDeserializer<IN> getDeserializer(Configuration conf) {
        // no op here as hoodie table source return row already, deserializer is no need.
        return null;
    }

    @Override
    public <T> FetchData<T> fetch(Partition partition, Optional<Offset> startOffset, FetchWindow windowInfo)
        throws IOException {
        if (windowInfo.getType() == WindowType.ALL_WINDOW) {
            FileSplit split = (FileSplit) partition;
            ParquetFormat format = new ParquetFormat();
            format.init(tableConf, tableSchema, split);
            Iterator<Row> iterator = format.batchRead();
            Offset nextOffset = new FileOffset(-1L);
            format.close();
            return (FetchData<T>) FetchData.createBatchFetch(iterator, nextOffset);
        } else {
            throw new GeaFlowDSLException("Hudi table source not support window:{}", windowInfo.getType());
        }
    }

    @Override
    public void close() {

    }
}
