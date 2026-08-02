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

package org.apache.geaflow.store.rocksdb;

import java.util.HashMap;
import java.util.Map;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.common.exception.GeaflowRuntimeException;
import org.apache.geaflow.state.graph.encoder.IGraphKVEncoder;
import org.apache.geaflow.store.rocksdb.proxy.ProxyBuilder;
import org.testng.Assert;
import org.testng.annotations.Test;

public class PartitionTypeTest {

    @Test
    public void testGetEnumIsCaseInsensitive() {
        Assert.assertEquals(PartitionType.getEnum("dt"), PartitionType.DT);
        Assert.assertEquals(PartitionType.getEnum("DT_LABEL"), PartitionType.DT_LABEL);
        Assert.assertEquals(PartitionType.getEnum("none"), PartitionType.NONE);
    }

    @Test(expectedExceptions = GeaflowRuntimeException.class)
    public void testGetEnumRejectsUnknownType() {
        PartitionType.getEnum("unknown");
    }

    @Test
    public void testPartitionFlags() {
        Assert.assertTrue(PartitionType.DT.isDtPartition());
        Assert.assertFalse(PartitionType.DT.isLabelPartition());
        Assert.assertTrue(PartitionType.DT.isPartition());

        Assert.assertTrue(PartitionType.DT_LABEL.isDtPartition());
        Assert.assertTrue(PartitionType.DT_LABEL.isLabelPartition());

        Assert.assertFalse(PartitionType.NONE.isPartition());
    }

    /**
     * DT_LABEL partition has no proxy implementation yet, so {@link ProxyBuilder} must reject it
     * fast with an explicit error that names the partition type, rather than falling through to a
     * generic "unexpected partition type" message. The rejection happens before the RocksDB
     * client or encoder are touched, so {@code null} arguments are fine here.
     */
    @Test
    public void testDtLabelPartitionIsRejectedWithClearMessage() {
        try {
            ProxyBuilder.build(newConfig("dt_label"), null, (IGraphKVEncoder<Object, Object, Object>) null);
            Assert.fail("expected DT_LABEL partition to be rejected");
        } catch (GeaflowRuntimeException e) {
            Assert.assertTrue(e.getMessage() != null && e.getMessage().contains("DT_LABEL"),
                "error message should name the unsupported partition type, but was: "
                    + e.getMessage());
        }
    }

    /**
     * DT partition is implemented (see {@code SyncGraphDtPartitionProxy}), so {@link ProxyBuilder}
     * must dispatch it to a real proxy instead of rejecting it. With a {@code null} client the
     * proxy constructor fails with a {@link NullPointerException}, which confirms the builder got
     * past partition-type dispatch rather than throwing a {@link GeaflowRuntimeException}.
     */
    @Test(expectedExceptions = NullPointerException.class)
    public void testDtPartitionIsDispatchedToProxy() {
        ProxyBuilder.build(newConfig("dt"), null, (IGraphKVEncoder<Object, Object, Object>) null);
    }

    private Configuration newConfig(String partitionType) {
        Map<String, String> map = new HashMap<>();
        map.put(RocksdbConfigKeys.ROCKSDB_GRAPH_STORE_PARTITION_TYPE.getKey(), partitionType);
        return new Configuration(map);
    }
}
