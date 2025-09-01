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
import java.util.Objects;
import org.apache.geaflow.api.context.RuntimeContext;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.common.config.keys.ConnectorConfigKeys;
import org.apache.geaflow.common.config.keys.DSLConfigKeys;
import org.apache.geaflow.dsl.common.data.Row;
import org.apache.geaflow.dsl.common.types.StructType;
import org.apache.geaflow.dsl.connector.api.TableSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileTableSink implements TableSink {

    private static final Logger LOGGER = LoggerFactory.getLogger(FileTableSink.class);

    private String path;

    private String separator;

    private StructType schema;

    private Configuration tableConf;

    protected transient FileWriteHandler writer;

    @Override
    public void init(Configuration tableConf, StructType schema) {
        this.path = tableConf.getString(ConnectorConfigKeys.GEAFLOW_DSL_FILE_PATH);
        this.separator = tableConf.getString(ConnectorConfigKeys.GEAFLOW_DSL_COLUMN_SEPARATOR);
        this.schema = Objects.requireNonNull(schema);
        this.tableConf = tableConf;
    }

    @Override
    public void open(RuntimeContext context) {
        this.writer = FileWriteHandlers.from(path, tableConf);
        this.writer.init(tableConf, schema, context.getTaskArgs().getTaskIndex());
    }

    @Override
    public void write(Row row) throws IOException {
        Object[] values = new Object[schema.size()];
        for (int i = 0; i < schema.size(); i++) {
            values[i] = row.getField(i, schema.getType(i));
        }

        StringBuilder line = new StringBuilder();
        for (Object value : values) {
            if (line.length() > 0) {
                line.append(separator);
            }
            line.append(value);
        }
        writer.write(line + "\n");
    }

    @Override
    public void finish() throws IOException {
        String split = tableConf.getString(DSLConfigKeys.TABLE_SINK_SPLIT_LINE.getKey(), null);
        if (split != null) {
            writer.write(split + "\n");
        }

        writer.flush();
    }

    @Override
    public void close() {
        if (writer != null) {
            try {
                writer.close();
            } catch (IOException e) {
                LOGGER.warn("Error in close writer", e);
            }
        }
    }
}
