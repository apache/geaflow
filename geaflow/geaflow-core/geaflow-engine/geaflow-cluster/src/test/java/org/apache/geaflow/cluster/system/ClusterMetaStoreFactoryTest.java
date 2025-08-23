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

package org.apache.geaflow.cluster.system;

import java.io.File;
import org.apache.commons.io.FileUtils;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.common.config.keys.ExecutionConfigKeys;
import org.apache.geaflow.common.config.keys.FrameworkConfigKeys;
import org.apache.geaflow.file.FileConfigKeys;
import org.testng.Assert;
import org.testng.annotations.Test;

public class ClusterMetaStoreFactoryTest {

    @Test
    public void testCreateMemoryKvStore() {
        Configuration configuration = new Configuration();
        configuration.put(FrameworkConfigKeys.SYSTEM_STATE_BACKEND_TYPE, "memory");
        IClusterMetaKVStore ikvStore = ClusterMetaStoreFactory.create("test", configuration);
        Assert.assertNotNull(ikvStore);
    }

    @Test
    public void testCreateRocksdbKvStore() {

        String path = "/tmp/" + ClusterMetaStoreFactoryTest.class.getSimpleName();
        FileUtils.deleteQuietly(new File(path));

        Configuration configuration = new Configuration();
        configuration.put(ExecutionConfigKeys.JOB_APP_NAME, ClusterMetaStoreFactoryTest.class.getSimpleName());
        configuration.put(FileConfigKeys.PERSISTENT_TYPE, "LOCAL");
        configuration.put(FileConfigKeys.ROOT, path);
        configuration.put(FrameworkConfigKeys.SYSTEM_STATE_BACKEND_TYPE, "rocksdb");
        configuration.put(ExecutionConfigKeys.SYSTEM_META_TABLE, "cluster_metastore_kv_test");
        IClusterMetaKVStore ikvStore = ClusterMetaStoreFactory.create("test", configuration);
        Assert.assertNotNull(ikvStore);
    }
}
