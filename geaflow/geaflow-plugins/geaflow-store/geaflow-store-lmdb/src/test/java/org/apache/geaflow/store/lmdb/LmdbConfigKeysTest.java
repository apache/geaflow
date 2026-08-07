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

package org.apache.geaflow.store.lmdb;

import org.apache.geaflow.common.config.Configuration;
import org.testng.Assert;
import org.testng.annotations.Test;

public class LmdbConfigKeysTest {

    @Test
    public void testCheckpointPath() {
        String path = "/tmp/lmdb/test";
        long checkpointId = 12345L;
        String chkPath = LmdbConfigKeys.getChkPath(path, checkpointId);
        Assert.assertEquals(chkPath, "/tmp/lmdb/test_chk12345");

        Assert.assertTrue(LmdbConfigKeys.isChkPath(chkPath));
        Assert.assertFalse(LmdbConfigKeys.isChkPath(path));

        String chkPathPrefix = LmdbConfigKeys.getChkPathPrefix(chkPath);
        Assert.assertEquals(chkPathPrefix, "/tmp/lmdb/test_chk");

        long extractedId = LmdbConfigKeys.getChkIdFromChkPath(chkPath);
        Assert.assertEquals(extractedId, checkpointId);
    }

    @Test
    public void testDatabaseConstants() {
        Assert.assertEquals(LmdbConfigKeys.VERTEX_CF, LmdbConfigKeys.VERTEX_DB);
        Assert.assertEquals(LmdbConfigKeys.EDGE_CF, LmdbConfigKeys.EDGE_DB);
        Assert.assertEquals(LmdbConfigKeys.VERTEX_INDEX_CF, LmdbConfigKeys.VERTEX_INDEX_DB);
        Assert.assertEquals(LmdbConfigKeys.DEFAULT_CF, LmdbConfigKeys.DEFAULT_DB);
    }

    @Test
    public void testDefaultConfigValues() {
        Configuration config = new Configuration();

        // Test map size default
        long mapSize = config.getLong(LmdbConfigKeys.LMDB_MAP_SIZE);
        Assert.assertEquals(mapSize, 107374182400L); // 100GB

        // Test max readers default
        int maxReaders = config.getInteger(LmdbConfigKeys.LMDB_MAX_READERS);
        Assert.assertEquals(maxReaders, 126);

        // Test sync mode default
        String syncMode = config.getString(LmdbConfigKeys.LMDB_SYNC_MODE);
        Assert.assertEquals(syncMode, "META_SYNC");

        // Test no TLS default
        boolean noTls = config.getBoolean(LmdbConfigKeys.LMDB_NO_TLS);
        Assert.assertFalse(noTls);

        // Test write map default
        boolean writeMap = config.getBoolean(LmdbConfigKeys.LMDB_WRITE_MAP);
        Assert.assertFalse(writeMap);

        // Test warning threshold default
        double threshold = config.getDouble(LmdbConfigKeys.LMDB_MAP_SIZE_WARNING_THRESHOLD);
        Assert.assertEquals(threshold, 0.9, 0.001);
    }

    @Test
    public void testConfigCompatibility() {
        // Test RocksDB compatibility aliases
        Assert.assertEquals(LmdbConfigKeys.ROCKSDB_PERSISTENT_CLEAN_THREAD_SIZE,
                          LmdbConfigKeys.LMDB_PERSISTENT_CLEAN_THREAD_SIZE);
        Assert.assertEquals(LmdbConfigKeys.ROCKSDB_GRAPH_STORE_PARTITION_TYPE,
                          LmdbConfigKeys.LMDB_GRAPH_STORE_PARTITION_TYPE);
    }
}
