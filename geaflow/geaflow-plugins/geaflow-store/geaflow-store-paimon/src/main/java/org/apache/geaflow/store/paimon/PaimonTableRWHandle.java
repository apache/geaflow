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

package org.apache.geaflow.store.paimon;

import java.util.ArrayList;
import java.util.List;
import java.util.OptionalLong;
import org.apache.geaflow.common.exception.GeaflowRuntimeException;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.reader.RecordReaderIterator;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.StreamTableCommit;
import org.apache.paimon.table.sink.StreamTableWrite;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.source.TableRead;
import org.apache.paimon.utils.Filter;

public class PaimonTableRWHandle {

    private Identifier identifier;

    private Table table;

    private StreamTableWrite streamTableWrite;

    private List<CommitMessage> commitMessages = new ArrayList<>();

    public PaimonTableRWHandle(Identifier identifier, Table table) {
        this.identifier = identifier;
        this.table = table;
        this.streamTableWrite = table.newStreamWriteBuilder().newWrite();
    }

    public void write(GenericRow row, int bucket) {
        try {
            streamTableWrite.write(row, bucket);
        } catch (Exception e) {
            throw new GeaflowRuntimeException("Failed to put data into Paimon.", e);
        }
    }

    public void commit(long checkpointId) {
        try (StreamTableCommit commit = table.newStreamWriteBuilder().newCommit()) {
            flush(checkpointId);
            commit.commit(checkpointId, commitMessages);
            commitMessages.clear();
        } catch (Exception e) {
            throw new GeaflowRuntimeException("Failed to commit data into Paimon.", e);
        }
    }

    public void rollbackTo(long snapshotId) {
        table.rollbackTo(snapshotId);
    }

    public long rollbackToLatest() {
        long latestSnapshotId = getLatestSnapshotId();
        if (latestSnapshotId < 0) {
            throw new GeaflowRuntimeException("Not found any valid snapshot version");
        }
        table.rollbackTo(latestSnapshotId);
        return latestSnapshotId;
    }

    public RecordReaderIterator<InternalRow> getIterator(Predicate predicate, Filter filter,
                                                         int[] projection) {
        try {
            ReadBuilder readBuilder = table.newReadBuilder().withProjection(projection);
            if (predicate != null) {
                readBuilder.withFilter(predicate);
            }
            List<Split> splits = readBuilder.newScan().plan().splits();
            TableRead tableRead = readBuilder.newRead();
            if (predicate != null) {
                tableRead.executeFilter();
            }
            RecordReader<InternalRow> reader = tableRead.createReader(splits);
            if (filter != null) {
                reader = reader.filter(filter);
            }
            return new RecordReaderIterator<>(reader);
        } catch (Exception e) {
            throw new GeaflowRuntimeException("Failed to get data from Paimon.", e);
        }
    }

    public void flush(long checkpointIdentifier) {
        try {
            this.commitMessages.addAll(streamTableWrite.prepareCommit(false, checkpointIdentifier));
        } catch (Exception e) {
            throw new GeaflowRuntimeException("Failed to flush data into Paimon.", e);
        }
    }

    public void close() {
        try {
            this.streamTableWrite.close();
        } catch (Exception e) {
            throw new GeaflowRuntimeException("Close stream table write failed.", e);
        }
    }

    public Table getTable() {
        return this.table;
    }

    public long getLatestSnapshotId() {
        OptionalLong latestCheckpoint = table.latestSnapshotId();
        if (latestCheckpoint.isPresent()) {
            return latestCheckpoint.getAsLong();
        } else {
            return -1L;
        }
    }

    public Identifier getIdentifier() {
        return this.identifier;
    }
}
