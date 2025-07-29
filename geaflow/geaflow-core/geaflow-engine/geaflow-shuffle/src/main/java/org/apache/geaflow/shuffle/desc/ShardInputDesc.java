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

package org.apache.geaflow.shuffle.desc;

import java.util.List;
import org.apache.geaflow.common.encoder.IEncoder;
import org.apache.geaflow.common.shuffle.BatchPhase;
import org.apache.geaflow.common.shuffle.DataExchangeMode;
import org.apache.geaflow.shuffle.message.Shard;

public class ShardInputDesc implements IInputDesc<Shard> {

    private final int edgeId;
    private final String edgeName;
    private final List<Shard> shards;

    private final IEncoder<?> encoder;
    private final DataExchangeMode dataExchangeMode;
    private final BatchPhase batchPhase;

    public ShardInputDesc(int edgeId,
                          String edgeName,
                          List<Shard> shards,
                          IEncoder<?> encoder,
                          DataExchangeMode dataExchangeMode,
                          BatchPhase batchPhase) {
        this.edgeId = edgeId;
        this.edgeName = edgeName;
        this.shards = shards;
        this.encoder = encoder;
        this.dataExchangeMode = dataExchangeMode;
        this.batchPhase = batchPhase;
    }

    @Override
    public int getEdgeId() {
        return edgeId;
    }

    @Override
    public String getName() {
        return edgeName;
    }

    @Override
    public List<Shard> getInput() {
        return shards;
    }

    @Override
    public InputType getInputType() {
        return InputType.META;
    }

    public IEncoder<?> getEncoder() {
        return encoder;
    }

    public DataExchangeMode getDataExchangeMode() {
        return this.dataExchangeMode;
    }

    public BatchPhase getBatchPhase() {
        return this.batchPhase;
    }

    public int getSliceNum() {
        return this.isPrefetchRead() ? 1 : this.shards.stream().mapToInt(s -> s.getSlices().size()).sum();
    }

    public boolean isPrefetchWrite() {
        return this.dataExchangeMode == DataExchangeMode.BATCH && this.batchPhase == BatchPhase.PREFETCH_WRITE;
    }

    public boolean isPrefetchRead() {
        return this.dataExchangeMode == DataExchangeMode.BATCH && this.batchPhase == BatchPhase.PREFETCH_READ;
    }

}
