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

package org.apache.geaflow.dsl.connector.odps;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.tunnel.TableTunnel;
import java.io.IOException;

public class PartitionWriter {

    private final TableTunnel.StreamRecordPack recordPack;
    private final int batchSize;
    private final int flushIntervalMs;
    private long lastFlushTime;

    public PartitionWriter(TableTunnel.StreamUploadSession uploadSession, int batchSize, int flushIntervalMs) {
        try {
            this.recordPack = uploadSession.newRecordPack();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        this.batchSize = batchSize;
        this.flushIntervalMs = flushIntervalMs;
        this.lastFlushTime = System.currentTimeMillis();
    }

    /**
     * Write a record to the stream, if the batch size is reached
     * or the flush interval is reached, flush the stream.
     * @param record The record to write.
     * @throws IOException If an I/O error occurs.
     */
    public void write(Record record) throws IOException {
        recordPack.append(record);
        if (recordPack.getRecordCount() >= batchSize
                || System.currentTimeMillis() - lastFlushTime > flushIntervalMs) {
            recordPack.flush();
            lastFlushTime = System.currentTimeMillis();
        }
    }

    /**
     * Flush the stream.
     * @throws IOException If an I/O error occurs.
     */
    public void flush() throws IOException {
        recordPack.flush();
    }
}
