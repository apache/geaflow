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

package org.apache.geaflow.dsl.connector.doris;

import org.apache.geaflow.common.config.Configuration;
import org.testng.Assert;
import org.testng.annotations.Test;

public class DorisConfigKeysTest {

    @Test
    public void testSinkDefaults() {
        Configuration conf = new Configuration();
        Assert.assertEquals(conf.getLong(DorisConfigKeys.GEAFLOW_DSL_DORIS_SINK_MAX_ROWS), 10000L);
        Assert.assertEquals(conf.getLong(DorisConfigKeys.GEAFLOW_DSL_DORIS_SINK_MAX_BYTES),
            10485760L);
        Assert.assertEquals(conf.getInteger(DorisConfigKeys.GEAFLOW_DSL_DORIS_SINK_MAX_RETRIES), 3);
        Assert.assertEquals(conf.getString(DorisConfigKeys.GEAFLOW_DSL_DORIS_SINK_FORMAT), "csv");
        Assert.assertEquals(conf.getString(DorisConfigKeys.GEAFLOW_DSL_DORIS_USERNAME), "root");
    }

    @Test
    public void testSourcePartitionDefaults() {
        Configuration conf = new Configuration();
        Assert.assertEquals(
            conf.getLong(DorisConfigKeys.GEAFLOW_DSL_DORIS_SOURCE_PARTITION_NUM), 1L);
        Assert.assertEquals(
            conf.getString(DorisConfigKeys.GEAFLOW_DSL_DORIS_SOURCE_PARTITION_COLUMN), "id");
    }

    @Test
    public void testOverrideValue() {
        Configuration conf = new Configuration();
        conf.put(DorisConfigKeys.GEAFLOW_DSL_DORIS_SINK_MAX_ROWS.getKey(), "500");
        Assert.assertEquals(conf.getLong(DorisConfigKeys.GEAFLOW_DSL_DORIS_SINK_MAX_ROWS), 500L);
    }
}
