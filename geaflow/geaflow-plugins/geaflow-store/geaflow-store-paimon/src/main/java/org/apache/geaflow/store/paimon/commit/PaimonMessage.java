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

package org.apache.geaflow.store.paimon.commit;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.Serializable;
import java.util.List;
import org.apache.geaflow.common.exception.GeaflowRuntimeException;
import org.apache.paimon.io.DataInputViewStreamWrapper;
import org.apache.paimon.io.DataOutputViewStreamWrapper;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PaimonMessage implements Serializable {

    private static final Logger LOGGER = LoggerFactory.getLogger(PaimonMessage.class);
    private static final ThreadLocal<CommitMessageSerializer> CACHE = ThreadLocal.withInitial(
        CommitMessageSerializer::new);

    private transient List<CommitMessage> messages;
    private byte[] messageBytes;
    private int serializerVersion;
    private String tableName;
    private long chkId;

    public PaimonMessage(long chkId, String tableName, List<CommitMessage> messages) {
        this.chkId = chkId;
        this.tableName = tableName;

        CommitMessageSerializer serializer = CACHE.get();
        this.serializerVersion = serializer.getVersion();

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try {
            serializer.serializeList(messages, new DataOutputViewStreamWrapper(out));
            this.messageBytes = out.toByteArray();
            LOGGER.info("ser bytes: {}", messageBytes.length);
        } catch (Exception e) {
            LOGGER.error("serialize message error", e);
            throw new GeaflowRuntimeException(e);
        }
    }

    public List<CommitMessage> getMessages() {
        if (messages == null) {
            CommitMessageSerializer serializer = CACHE.get();
            try {
                if (messageBytes == null) {
                    LOGGER.warn("deserialize message error, null");
                }
                ByteArrayInputStream in = new ByteArrayInputStream(messageBytes);
                messages = serializer.deserializeList(serializerVersion,
                    new DataInputViewStreamWrapper(in));
            } catch (Exception e) {
                LOGGER.error("deserialize message error", e);
                throw new GeaflowRuntimeException(e);
            }
        }
        return messages;
    }

    public String getTableName() {
        return tableName;
    }

    public long getCheckpointId() {
        return chkId;
    }
}
