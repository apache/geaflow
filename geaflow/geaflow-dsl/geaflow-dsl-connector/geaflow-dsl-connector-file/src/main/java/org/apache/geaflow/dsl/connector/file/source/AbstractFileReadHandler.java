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
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.common.config.keys.ConnectorConfigKeys;
import org.apache.geaflow.dsl.common.exception.GeaFlowDSLException;
import org.apache.geaflow.dsl.common.types.TableSchema;
import org.apache.geaflow.dsl.connector.api.FetchData;
import org.apache.geaflow.dsl.connector.api.serde.TableDeserializer;
import org.apache.geaflow.dsl.connector.file.source.FileTableSource.FileOffset;
import org.apache.geaflow.dsl.connector.file.source.FileTableSource.FileSplit;
import org.apache.geaflow.dsl.connector.file.source.format.FileFormat;
import org.apache.geaflow.dsl.connector.file.source.format.FileFormats;
import org.apache.geaflow.dsl.connector.file.source.format.StreamFormat;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractFileReadHandler implements FileReadHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractFileReadHandler.class);

    protected String formatName;
    protected Configuration tableConf;
    private TableSchema tableSchema;
    protected Path path;

    protected Map<FileSplit, FileFormat> fileFormats = new HashMap<>();

    @Override
    public void init(Configuration tableConf, TableSchema tableSchema, String path) throws IOException {
        this.formatName = tableConf.getString(ConnectorConfigKeys.GEAFLOW_DSL_FILE_FORMAT);
        this.tableConf = tableConf;
        this.tableSchema = tableSchema;
        this.path = new Path(path);
    }

    @Override
    public <T> FetchData<T> readPartition(FileSplit split, FileOffset offset, int windowSize) throws IOException {
        FileFormat<T> format = getFileFormat(split, offset);
        if (windowSize == Integer.MAX_VALUE) { // read all data from file
            Iterator<T> iterator = format.batchRead();
            return FetchData.createBatchFetch(iterator, new FileOffset(-1));
        } else {
            if (!(format instanceof StreamFormat)) {
                throw new GeaFlowDSLException("Format '{}' is not a stream format, the window size should be -1",
                    formatName);
            }
            StreamFormat<T> streamFormat = (StreamFormat<T>) format;
            return streamFormat.streamRead(offset, windowSize);
        }
    }

    private <T> FileFormat<T> getFileFormat(FileSplit split, FileOffset offset) throws IOException {
        if (!fileFormats.containsKey(split)) {
            FileFormat<T> fileFormat = FileFormats.loadFileFormat(formatName);
            fileFormat.init(tableConf, tableSchema, split);
            // skip pre offset for stream read.
            if (fileFormat instanceof StreamFormat) {
                ((StreamFormat) fileFormat).skip(offset.getOffset());
            }
            fileFormats.put(split, fileFormat);
        }
        return fileFormats.get(split);
    }

    @Override
    public void close() throws IOException {
        for (FileFormat format : fileFormats.values()) {
            format.close();
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> TableDeserializer<T> getDeserializer() {
        return (TableDeserializer<T>) FileFormats.loadFileFormat(formatName).getDeserializer();
    }

    public int findLineSplitSize(InputStream inputStream) {
        try {
            if (this.formatName.equalsIgnoreCase(SourceConstants.PARQUET)) {
                return 1;
            } else {
                int lineSplitSize = 1;
                int c;
                while (true) {
                    c = inputStream.read();
                    if (c == -1) {
                        break;
                    } else if (c == '\n') {
                        break;
                    } else if (c == '\r') {
                        int c2 = inputStream.read();
                        if (c2 == '\n') {
                            lineSplitSize = 2;
                            break;
                        } else if (c2 == -1) {
                            break;
                        }
                    }
                }
                return lineSplitSize;
            }
        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
            throw new RuntimeException(e);
        }
    }

    protected long findNextStartPos(long expectPos, long totalLength, InputStream inputStream) throws Exception {
        if (expectPos >= totalLength) {
            return totalLength;
        }
        inputStream.skip(expectPos);
        byte[] buffer = new byte[1];
        int readSize = 0;
        do {
            readSize = inputStream.read(buffer);
            if (readSize != -1) {
                expectPos++;
            }
        } while (readSize != -1 && !"\n".equalsIgnoreCase(new String(buffer)));
        if (expectPos >= totalLength) {
            expectPos = totalLength;
        }
        return expectPos;
    }
}
