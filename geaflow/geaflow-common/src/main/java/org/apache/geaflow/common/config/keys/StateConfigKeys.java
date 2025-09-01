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

package org.apache.geaflow.common.config.keys;

import java.io.Serializable;
import org.apache.geaflow.common.config.ConfigKey;
import org.apache.geaflow.common.config.ConfigKeys;

public class StateConfigKeys implements Serializable {

    public static final byte[] DELIMITER = "\u0001\u0008".getBytes();

    public static final ConfigKey STATE_ARCHIVED_VERSION_NUM = ConfigKeys
        .key("geaflow.state.archived.version.num")
        .defaultValue(1)
        .description("state archived version number, default 1");

    public static final ConfigKey STATE_PARANOID_CHECK_ENABLE = ConfigKeys
        .key("geaflow.state.paranoid.check.enable")
        .defaultValue(false)
        .description("state paranoid check, default false");

    public static final ConfigKey STATE_WRITE_ASYNC_ENABLE = ConfigKeys
        .key("geaflow.state.write.async.enable")
        .defaultValue(true)
        .description("state async write, default true");

    public static final ConfigKey STATE_WRITE_BUFFER_DEEP_COPY = ConfigKeys
        .key("geaflow.state.write.buffer.deep.copy")
        .defaultValue(false)
        .description("state write buffer deep copy read, default false");

    public static final ConfigKey STATE_WRITE_BUFFER_NUMBER = ConfigKeys
        .key("geaflow.state.write.buffer.number")
        .defaultValue(3)
        .description("state write buffer number, default 3");

    public static final ConfigKey STATE_WRITE_BUFFER_SIZE = ConfigKeys
        .key("geaflow.state.write.buffer.size")
        .defaultValue(10000)
        .description("state write buffer size, default 10000");

    public static final ConfigKey STATE_KV_ENCODER_CLASS = ConfigKeys
        .key("geaflow.state.kv.encoder.class")
        .defaultValue("org.apache.geaflow.state.graph.encoder.GraphKVEncoder")
        .description("state kv encoder");

    public static final ConfigKey STATE_KV_ENCODER_EDGE_ORDER = ConfigKeys
        .key("geaflow.state.kv.encoder.edge.order")
        .defaultValue("")
        .description("state kv encoder edge atom order, splitter ,");

    // for read only state.
    public static final ConfigKey STATE_RECOVER_LATEST_VERSION_ENABLE = ConfigKeys
        .key("geaflow.state.recover.latest.version.enable")
        .defaultValue(false)
        .description("enable recover latest version, default false");

    public static final ConfigKey STATE_BACKGROUND_SYNC_ENABLE = ConfigKeys
        .key("geaflow.state.background.sync.enable")
        .defaultValue(false)
        .description("enable state background sync, default false");

    public static final ConfigKey STATE_SYNC_GAP_MS = ConfigKeys
        .key("geaflow.state.sync.gap.ms")
        .defaultValue(600000)
        .description("state background sync ms, default 600000ms");

    public static final ConfigKey STATE_ROCKSDB_PERSIST_TIMEOUT_SECONDS = ConfigKeys
        .key("geaflow.state.rocksdb.persist.timeout.second")
        .defaultValue(Integer.MAX_VALUE)
        .description("rocksdb persist timeout second, default Integer.MAX_VALUE");
}
