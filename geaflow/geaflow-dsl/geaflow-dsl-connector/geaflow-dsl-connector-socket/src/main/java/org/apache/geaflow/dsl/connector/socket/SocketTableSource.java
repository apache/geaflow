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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.LinkedBlockingQueue;
import org.apache.geaflow.api.context.RuntimeContext;
import org.apache.geaflow.api.window.WindowType;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.common.utils.SleepUtils;
import org.apache.geaflow.dsl.common.exception.GeaFlowDSLException;
import org.apache.geaflow.dsl.common.types.TableSchema;
import org.apache.geaflow.dsl.connector.api.FetchData;
import org.apache.geaflow.dsl.connector.api.ISkipOpenAndClose;
import org.apache.geaflow.dsl.connector.api.Offset;
import org.apache.geaflow.dsl.connector.api.Partition;
import org.apache.geaflow.dsl.connector.api.TableSource;
import org.apache.geaflow.dsl.connector.api.serde.DeserializerFactory;
import org.apache.geaflow.dsl.connector.api.serde.TableDeserializer;
import org.apache.geaflow.dsl.connector.api.window.FetchWindow;
import org.apache.geaflow.dsl.connector.socket.server.NettySourceClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SocketTableSource implements TableSource, ISkipOpenAndClose {

    private static final Logger LOGGER = LoggerFactory.getLogger(SocketTableSource.class.getName());

    private Configuration tableConf;

    private LinkedBlockingQueue<String> dataQueue;

    @Override
    public void init(Configuration tableConf, TableSchema tableSchema) {
        this.tableConf = tableConf;
    }

    @Override
    public void open(RuntimeContext context) {
        String host = tableConf.getString(SocketConfigKeys.GEAFLOW_DSL_SOCKET_HOST);
        int port = tableConf.getInteger(SocketConfigKeys.GEAFLOW_DSL_SOCKET_PORT);
        this.dataQueue = new LinkedBlockingQueue<>();
        while (true) {
            try {
                NettySourceClient client = new NettySourceClient(host, port, dataQueue);
                client.run();
                break;
            } catch (Exception e) {
                LOGGER.info("Attempt to connect source netty server.");
                SleepUtils.sleepSecond(5);
            }
        }
    }

    @Override
    public List<Partition> listPartitions() {
        return Collections.singletonList(new SocketPartition(new ArrayList<>()));
    }

    @Override
    public List<Partition> listPartitions(int parallelism) {
        return listPartitions();
    }

    @Override
    public <IN> TableDeserializer<IN> getDeserializer(Configuration conf) {
        return DeserializerFactory.loadTextDeserializer();
    }

    @Override
    public <T> FetchData<T> fetch(Partition partition, Optional<Offset> startOffset,
                                  FetchWindow windowInfo) throws IOException {
        if (windowInfo.getType() != WindowType.SIZE_TUMBLING_WINDOW) {
            throw new GeaFlowDSLException("Not support window type:{}", windowInfo.getType());
        }
        try {
            List<String> fetchData = new ArrayList<>();
            for (int i = 0; i < windowInfo.windowSize(); i++) {
                fetchData.add(dataQueue.take());
            }
            return (FetchData<T>) FetchData.createStreamFetch(fetchData, new SocketOffset(), false);
        } catch (InterruptedException e) {
            throw new IOException(e);
        }
    }

    @Override
    public void close() {

    }

    public static class SocketPartition implements Partition {

        private List<String> data;

        public SocketPartition(List<String> data) {
            this.data = data;
        }

        public List<String> getData() {
            return data;
        }

        @Override
        public String getName() {
            return String.valueOf(data.hashCode());
        }

        @Override
        public void setIndex(int index, int parallel) {
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            SocketPartition that = (SocketPartition) o;
            return Objects.equals(data, that.data);
        }

        @Override
        public int hashCode() {
            return Objects.hash(data);
        }
    }

    public static class SocketOffset implements Offset {

        public SocketOffset() {

        }

        @Override
        public String humanReadable() {
            return "None";
        }

        @Override
        public long getOffset() {
            return -1;
        }

        @Override
        public boolean isTimestamp() {
            return false;
        }
    }
}
