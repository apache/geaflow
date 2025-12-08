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

package org.apache.geaflow.store.lmdb.proxy;

import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.common.config.keys.StateConfigKeys;
import org.apache.geaflow.common.exception.GeaflowRuntimeException;
import org.apache.geaflow.state.graph.encoder.IGraphKVEncoder;
import org.apache.geaflow.store.lmdb.LmdbClient;
import org.apache.geaflow.store.lmdb.LmdbConfigKeys;
import org.apache.geaflow.store.lmdb.PartitionType;

public class ProxyBuilder {

    public static <K, VV, EV> IGraphLmdbProxy<K, VV, EV> build(
        Configuration config, LmdbClient lmdbClient,
        IGraphKVEncoder<K, VV, EV> encoder) {
        PartitionType partitionType = PartitionType.getEnum(
            config.getString(LmdbConfigKeys.ROCKSDB_GRAPH_STORE_PARTITION_TYPE));
        if (partitionType.isPartition()) {
            // TODO: Partition proxies not yet supported in LMDB implementation
            throw new GeaflowRuntimeException("Partitioned proxies not yet supported for LMDB. "
                + "Partition type requested: " + partitionType);
        } else {
            if (config.getBoolean(StateConfigKeys.STATE_WRITE_ASYNC_ENABLE)) {
                // TODO: Async proxies not yet supported in LMDB implementation
                throw new GeaflowRuntimeException("Async write mode not yet supported for LMDB. "
                    + "Please disable STATE_WRITE_ASYNC_ENABLE.");
            } else {
                return new SyncGraphLmdbProxy<>(lmdbClient, encoder, config);
            }
        }
    }

    public static <K, VV, EV> IGraphMultiVersionedLmdbProxy<K, VV, EV> buildMultiVersioned(
        Configuration config, LmdbClient lmdbClient,
        IGraphKVEncoder<K, VV, EV> encoder) {
        if (config.getBoolean(StateConfigKeys.STATE_WRITE_ASYNC_ENABLE)) {
            // TODO: Async proxies not yet supported in LMDB implementation
            throw new GeaflowRuntimeException("Async write mode not yet supported for LMDB. "
                + "Please disable STATE_WRITE_ASYNC_ENABLE.");
        } else {
            return new SyncGraphMultiVersionedProxy<>(lmdbClient, encoder, config);
        }
    }
}
