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

package org.apache.geaflow.dsl.connector.file.source.format;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.common.config.keys.ConnectorConfigKeys;
import org.apache.geaflow.dsl.common.exception.GeaFlowDSLException;
import org.apache.geaflow.dsl.common.types.TableSchema;
import org.apache.geaflow.dsl.connector.api.FetchData;
import org.apache.geaflow.dsl.connector.api.serde.TableDeserializer;
import org.apache.geaflow.dsl.connector.api.serde.impl.TextDeserializer;
import org.apache.geaflow.dsl.connector.file.source.FileTableSource.FileOffset;
import org.apache.geaflow.dsl.connector.file.source.FileTableSource.FileSplit;
import org.apache.geaflow.dsl.connector.file.source.SourceConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TextFormat implements FileFormat<String>, StreamFormat<String> {

    private static final Logger LOGGER = LoggerFactory.getLogger(TextFormat.class);

    private BufferedReader reader;

    private Configuration tableConf;
    protected boolean singleFileModeRead = false;
    private FileSplit fileSplit;
    private long readCnt = 0L;
    private long readSize = 0L;
    private long expectReadSize = -1L;
    private int lineSplitSize = 1;
    private boolean firstRead = true;

    @Override
    public String getFormat() {
        return SourceConstants.TXT;
    }

    @Override
    public void init(Configuration tableConf, TableSchema tableSchema, FileSplit split) throws IOException {
        this.tableConf = tableConf;
        this.fileSplit = split;
        this.expectReadSize = split.getSplitLength();
        this.lineSplitSize = split.getLineSplitSize();
        this.reader = new BufferedReader(new InputStreamReader(split.openStream(tableConf)));
        this.expectReadSize = split.getSplitLength();
        this.lineSplitSize = split.getLineSplitSize();
        this.singleFileModeRead = tableConf.getBoolean(ConnectorConfigKeys.GEAFLOW_DSL_SOURCE_FILE_PARALLEL_MOD);
    }

    @Override
    public Iterator<String> batchRead() {
        return new Iterator<String>() {

            private String current = null;

            @Override
            public boolean hasNext() {
                if (singleFileModeRead) {
                    throw new GeaFlowDSLException("Single file mode read is not supported in batch read");
                }
                try {
                    if (expectReadSize != -1L && readSize >= expectReadSize) {
                        return false;
                    }
                    if (current == null) {
                        current = reader.readLine();
                    }
                    return current != null;
                } catch (IOException e) {
                    throw new GeaFlowDSLException("Error in read", e);
                }
            }

            @Override
            public String next() {
                String next = current;
                current = null;
                readSize += next.length() + lineSplitSize;
                return next;
            }
        };
    }

    @Override
    public FetchData<String> streamRead(FileOffset offset, int windowSize) throws IOException {
        if (firstRead) {
            firstRead = false;
            close();
            this.reader = new BufferedReader(new InputStreamReader(fileSplit.openStream(tableConf, offset.getOffset())));
        }

        List<String> readContents = new ArrayList<>(windowSize);
        long nextOffset = offset.getOffset();
        int i = 0;
        for (i = 0; i < windowSize; i++) {
            String line = reader.readLine();
            if (line == null) {
                break;
            }
            if (!singleFileModeRead || readCnt % this.fileSplit.getParallel() == this.fileSplit.getIndex()) {
                readContents.add(line);
            }
            nextOffset += line.length() + lineSplitSize;
            readSize += line.length() + lineSplitSize;
            readCnt++;
            if (fileSplit.getSplitStart() != -1L && nextOffset >= fileSplit.getSplitLength()) {
                break;
            }
        }
        boolean isFinished = i < windowSize;
        return FetchData.createStreamFetch(readContents, new FileOffset(nextOffset), isFinished);
    }

    @Override
    public void close() throws IOException {
        if (reader != null) {
            reader.close();
        }
    }

    @Override
    public TableDeserializer<String> getDeserializer() {
        return new TextDeserializer();
    }

    @Override
    public void skip(long n) throws IOException {
        reader.skip(n);
    }
}
