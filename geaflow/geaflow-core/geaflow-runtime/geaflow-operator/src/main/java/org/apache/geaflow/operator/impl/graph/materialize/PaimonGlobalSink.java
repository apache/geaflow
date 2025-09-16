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

package org.apache.geaflow.operator.impl.graph.materialize;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.geaflow.api.context.RuntimeContext;
import org.apache.geaflow.api.function.RichWindowFunction;
import org.apache.geaflow.api.function.io.SinkFunction;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.common.config.keys.ExecutionConfigKeys;
import org.apache.geaflow.common.exception.GeaflowRuntimeException;
import org.apache.geaflow.store.paimon.PaimonCatalogClient;
import org.apache.geaflow.store.paimon.PaimonCatalogManager;
import org.apache.geaflow.store.paimon.commit.PaimonMessage;
import org.apache.geaflow.store.paimon.config.PaimonConfigKeys;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.StreamTableCommit;
import org.apache.paimon.table.sink.StreamWriteBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PaimonGlobalSink extends RichWindowFunction implements SinkFunction<PaimonMessage> {

    private static final Logger LOGGER = LoggerFactory.getLogger(PaimonGlobalSink.class);

    private String dbName;
    private String jobName;
    private long windowId;

    private List<PaimonMessage> paimonMessages;
    private Map<String, StreamWriteBuilder> writeBuilders;
    private PaimonCatalogClient client;

    @Override
    public void open(RuntimeContext runtimeContext) {
        this.windowId = runtimeContext.getWindowId();
        Configuration jobConfig = runtimeContext.getConfiguration();
        this.client = PaimonCatalogManager.getCatalogClient(jobConfig);
        this.jobName = jobConfig.getString(ExecutionConfigKeys.JOB_APP_NAME);
        this.dbName = jobConfig.getString(PaimonConfigKeys.PAIMON_STORE_DATABASE);

        this.writeBuilders = new HashMap<>();
        this.paimonMessages = new ArrayList<>();
        LOGGER.info("init paimon sink with db {}", dbName);
    }

    @Override
    public void write(PaimonMessage message) throws Exception {
        paimonMessages.add(message);
    }

    @Override
    public void finish() {
        if (paimonMessages.isEmpty()) {
            LOGGER.info("commit windowId {} empty messages", windowId);
            return;
        }

        final long startTime = System.currentTimeMillis();
        final long checkpointId = paimonMessages.get(0).getCheckpointId();
        Map<String, List<CommitMessage>> tableMessages = new HashMap<>();
        for (PaimonMessage message : paimonMessages) {
            List<CommitMessage> messages = message.getMessages();
            if (messages != null && !messages.isEmpty()) {
                String tableName = message.getTableName();
                List<CommitMessage> commitMessages = tableMessages.computeIfAbsent(tableName,
                    k -> new ArrayList<>());
                commitMessages.addAll(messages);
            }
        }
        long deserializeTime = System.currentTimeMillis() - startTime;
        if (!tableMessages.isEmpty()) {
            for (Map.Entry<String, List<CommitMessage>> entry : tableMessages.entrySet()) {
                LOGGER.info("commit table:{} messages:{}", entry.getKey(), entry.getValue().size());
                StreamWriteBuilder writeBuilder = getWriteBuilder(entry.getKey());
                try (StreamTableCommit commit = writeBuilder.newCommit()) {
                    try {
                        commit.commit(checkpointId, entry.getValue());
                    } catch (Throwable e) {
                        LOGGER.warn("commit failed: {}", e.getMessage(), e);
                        Map<Long, List<CommitMessage>> commitIdAndMessages = new HashMap<>();
                        commitIdAndMessages.put(checkpointId, entry.getValue());
                        commit.filterAndCommit(commitIdAndMessages);
                    }
                } catch (Throwable e) {
                    LOGGER.error("Failed to commit data into Paimon: {}", e.getMessage(), e);
                    throw new GeaflowRuntimeException("Failed to commit data into Paimon.", e);
                }
            }
        }
        LOGGER.info("committed chkId:{} messages:{} deserializeCost:{}ms",
            checkpointId, paimonMessages.size(), deserializeTime);
        paimonMessages.clear();
    }

    private StreamWriteBuilder getWriteBuilder(String tableName) {
        return writeBuilders.computeIfAbsent(tableName, k -> {
            try {
                FileStoreTable table = (FileStoreTable) client.getTable(Identifier.create(dbName,
                    tableName));
                return table.newStreamWriteBuilder().withCommitUser(jobName);
            } catch (Throwable e) {
                throw new GeaflowRuntimeException(e);
            }
        });
    }

    @Override
    public void close() {
        LOGGER.info("close sink");
        if (client != null) {
            client.close();
        }
    }

}
