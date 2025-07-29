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

package org.apache.geaflow.shuffle.api.writer;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import org.apache.geaflow.common.metric.ShuffleWriteMetrics;
import org.apache.geaflow.common.shuffle.DataExchangeMode;
import org.apache.geaflow.shuffle.message.Shard;
import org.apache.geaflow.shuffle.network.IConnectionManager;

public class PipelineWriter<T> implements IShuffleWriter<T, Shard> {

    private final IConnectionManager connectionManager;
    private ShardWriter<T, Shard> shardWriter;

    public PipelineWriter(IConnectionManager connectionManager) {
        this.connectionManager = connectionManager;
    }

    @Override
    public void init(IWriterContext writerContext) {
        this.shardWriter = writerContext.getDataExchangeMode() == DataExchangeMode.BATCH
            ? new SpillableShardWriter<>(this.connectionManager.getShuffleAddress())
            : new PipelineShardWriter<>();
        this.shardWriter.init(writerContext);
    }

    @Override
    public void emit(long batchId, T value, boolean isRetract, int[] channels) throws IOException {
        this.shardWriter.emit(batchId, value, isRetract, channels);
    }

    @Override
    public void emit(long batchId, List<T> data, boolean isRetract, int channel) throws IOException {
        this.shardWriter.emit(batchId, data, channel);
    }

    @Override
    public Optional<Shard> flush(long batchId) throws IOException {
        return this.shardWriter.finish(batchId);
    }

    @Override
    public ShuffleWriteMetrics getShuffleWriteMetrics() {
        return this.shardWriter.getShuffleWriteMetrics();
    }

    @Override
    public void close() {
        if (this.shardWriter != null) {
            this.shardWriter.close();
        }
    }

}
