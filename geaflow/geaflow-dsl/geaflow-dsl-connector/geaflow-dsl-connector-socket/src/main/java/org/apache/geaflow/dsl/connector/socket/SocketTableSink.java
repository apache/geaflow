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

package org.apache.geaflow.dsl.connector.socket;

import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.LinkedBlockingQueue;
import org.apache.geaflow.api.context.RuntimeContext;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.common.config.keys.ConnectorConfigKeys;
import org.apache.geaflow.common.utils.SleepUtils;
import org.apache.geaflow.dsl.common.data.Row;
import org.apache.geaflow.dsl.common.types.StructType;
import org.apache.geaflow.dsl.connector.api.ISkipOpenAndClose;
import org.apache.geaflow.dsl.connector.api.TableSink;
import org.apache.geaflow.dsl.connector.socket.server.NettySinkClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SocketTableSink implements TableSink, ISkipOpenAndClose {

    private static final Logger LOGGER = LoggerFactory.getLogger(SocketTableSink.class.getName());

    private Configuration tableConf;

    private StructType schema;

    private String separator;

    private LinkedBlockingQueue<String> dataQueue;

    @Override
    public void init(Configuration tableConf, StructType schema) {
        this.tableConf = tableConf;
        this.schema = Objects.requireNonNull(schema);
        this.separator = tableConf.getString(ConnectorConfigKeys.GEAFLOW_DSL_COLUMN_SEPARATOR);
    }

    @Override
    public void open(RuntimeContext context) {
        String host = tableConf.getString(SocketConfigKeys.GEAFLOW_DSL_SOCKET_HOST);
        int port = tableConf.getInteger(SocketConfigKeys.GEAFLOW_DSL_SOCKET_PORT);
        this.dataQueue = new LinkedBlockingQueue<>();
        while (true) {
            try {
                NettySinkClient client = new NettySinkClient(host, port, dataQueue);
                client.run();
                break;
            } catch (Exception e) {
                LOGGER.info("Attempt to connect sink netty server.");
                SleepUtils.sleepSecond(5);
            }
        }
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
        try {
            dataQueue.put(line.toString());
        } catch (InterruptedException e) {
            LOGGER.info(null, e);
        }

    }

    @Override
    public void finish() throws IOException {

    }

    @Override
    public void close() {

    }
}
