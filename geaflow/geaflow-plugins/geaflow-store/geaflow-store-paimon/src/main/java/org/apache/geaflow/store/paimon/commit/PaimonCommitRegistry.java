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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.paimon.table.sink.CommitMessage;

public class PaimonCommitRegistry {

    private static PaimonCommitRegistry instance;

    private final ConcurrentHashMap<Integer, List<TaskCommitMessage>> index2CommitMessages =
        new ConcurrentHashMap<>();

    public static synchronized PaimonCommitRegistry initInstance() {
        if (instance == null) {
            instance = new PaimonCommitRegistry();
        }
        return instance;
    }

    public static synchronized PaimonCommitRegistry getInstance() {
        return instance;
    }

    public synchronized void addMessages(int index, String tableName,
                                         List<CommitMessage> commitMessages) {
        List<TaskCommitMessage> message = index2CommitMessages.computeIfAbsent(index,
            key -> new ArrayList<>());
        message.add(new TaskCommitMessage(tableName, commitMessages));
    }

    public synchronized List<TaskCommitMessage> pollMessages(int index) {
        List<TaskCommitMessage> messages = index2CommitMessages.get(index);
        if (messages != null && !messages.isEmpty()) {
            List<TaskCommitMessage> result = new ArrayList<>(messages);
            messages.clear();
            return result;
        }
        return messages;
    }

    public static class TaskCommitMessage implements Serializable {
        private final String tableName;
        private final List<CommitMessage> messages;

        public TaskCommitMessage(String tableName, List<CommitMessage> messages) {
            this.tableName = tableName;
            this.messages = messages;
        }

        public String getTableName() {
            return tableName;
        }

        public List<CommitMessage> getMessages() {
            return messages;
        }
    }
}
