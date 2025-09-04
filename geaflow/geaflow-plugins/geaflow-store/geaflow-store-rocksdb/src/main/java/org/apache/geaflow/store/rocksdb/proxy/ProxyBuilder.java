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

package org.apache.geaflow.store.rocksdb.proxy;

import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.common.config.keys.StateConfigKeys;
import org.apache.geaflow.common.exception.GeaflowRuntimeException;
import org.apache.geaflow.state.graph.encoder.IGraphKVEncoder;
import org.apache.geaflow.store.rocksdb.PartitionType;
import org.apache.geaflow.store.rocksdb.RocksdbClient;
import org.apache.geaflow.store.rocksdb.RocksdbConfigKeys;

public class ProxyBuilder {

    public static <K, VV, EV> IGraphRocksdbProxy<K, VV, EV> build(
        Configuration config, RocksdbClient rocksdbClient,
        IGraphKVEncoder<K, VV, EV> encoder) {
        PartitionType partitionType = PartitionType.getEnum(
            config.getString(RocksdbConfigKeys.ROCKSDB_GRAPH_STORE_PARTITION_TYPE));
        if (partitionType.isPartition()) {
            if (partitionType == PartitionType.LABEL) {
                // Support async graph proxy partitioned by label
                if (config.getBoolean(StateConfigKeys.STATE_WRITE_ASYNC_ENABLE)) {
                    // TODO: Implement AsyncGraphLabelPartitionProxy
                    // For now, fall back to sync implementation
                    return new SyncGraphLabelPartitionProxy<>(rocksdbClient, encoder, config);
                } else {
                    return new SyncGraphLabelPartitionProxy<>(rocksdbClient, encoder, config);
                }
            } else if (partitionType == PartitionType.DT) {
                return new SyncGraphDtPartitionProxy<>(rocksdbClient, encoder, config);
            }
            throw new GeaflowRuntimeException("unexpected partition type: " + config.getString(
                RocksdbConfigKeys.ROCKSDB_GRAPH_STORE_PARTITION_TYPE));
        } else {
            if (config.getBoolean(StateConfigKeys.STATE_WRITE_ASYNC_ENABLE)) {
                return new AsyncGraphRocksdbProxy<>(rocksdbClient, encoder, config);
            } else {
                return new SyncGraphRocksdbProxy<>(rocksdbClient, encoder, config);
            }
        }
    }

    public static <K, VV, EV> IGraphMultiVersionedRocksdbProxy<K, VV, EV> buildMultiVersioned(
        Configuration config, RocksdbClient rocksdbClient,
        IGraphKVEncoder<K, VV, EV> encoder) {
        if (config.getBoolean(StateConfigKeys.STATE_WRITE_ASYNC_ENABLE)) {
            return new AsyncGraphMultiVersionedProxy<>(rocksdbClient, encoder, config);
        } else {
            return new SyncGraphMultiVersionedProxy<>(rocksdbClient, encoder, config);
        }
    }
}
