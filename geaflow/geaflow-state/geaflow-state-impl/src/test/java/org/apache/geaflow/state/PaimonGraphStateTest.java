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

package org.apache.geaflow.state;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.common.config.keys.ExecutionConfigKeys;
import org.apache.geaflow.common.config.keys.StateConfigKeys;
import org.apache.geaflow.common.type.IType;
import org.apache.geaflow.common.type.primitive.StringType;
import org.apache.geaflow.common.utils.GsonUtil;
import org.apache.geaflow.file.FileConfigKeys;
import org.apache.geaflow.model.graph.edge.impl.ValueEdge;
import org.apache.geaflow.model.graph.meta.GraphElementMetas;
import org.apache.geaflow.model.graph.meta.GraphMeta;
import org.apache.geaflow.model.graph.meta.GraphMetaType;
import org.apache.geaflow.model.graph.vertex.impl.ValueVertex;
import org.apache.geaflow.state.descriptor.GraphStateDescriptor;
import org.apache.geaflow.store.paimon.PaimonConfigKeys;
import org.apache.geaflow.utils.keygroup.DefaultKeyGroupAssigner;
import org.apache.geaflow.utils.keygroup.KeyGroup;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class PaimonGraphStateTest {

    static Map<String, String> config = new HashMap<>();

    @BeforeClass
    public static void setUp() {
        FileUtils.deleteQuietly(new File("/tmp/geaflow/chk/"));
        FileUtils.deleteQuietly(new File("/tmp/PaimonGraphStateTest"));
        Map<String, String> persistConfig = new HashMap<>();
        config.put(ExecutionConfigKeys.JOB_APP_NAME.getKey(),
            "PaimonGraphStateTest" + System.currentTimeMillis());
        config.put(FileConfigKeys.PERSISTENT_TYPE.getKey(), "LOCAL");
        config.put(FileConfigKeys.ROOT.getKey(), "/tmp/geaflow/chk/");
        config.put(FileConfigKeys.JSON_CONFIG.getKey(), GsonUtil.toJson(persistConfig));
        config.put(PaimonConfigKeys.PAIMON_OPTIONS_WAREHOUSE.getKey(),
            "file:///tmp/PaimonGraphStateTest/");
    }

    @AfterClass
    public static void tearUp() {
        FileUtils.deleteQuietly(new File("/tmp/PaimonGraphStateTest"));
    }

    private <T> GraphState<T, T, T> getGraphState(IType<T> type, String name,
                                                  Map<String, String> conf) {
        return getGraphState(type, name, conf, new KeyGroup(0, 1), 2);
    }

    private <T> GraphState<T, T, T> getGraphState(IType<T> type, String name,
                                                  Map<String, String> conf, KeyGroup keyGroup,
                                                  int maxPara) {
        GraphElementMetas.clearCache();
        GraphMetaType tag = new GraphMetaType(type, ValueVertex.class, ValueVertex::new,
            type.getTypeClass(), ValueEdge.class, ValueEdge::new, type.getTypeClass());

        GraphStateDescriptor desc = GraphStateDescriptor.build(name, StoreType.PAIMON.name());
        desc.withKeyGroup(keyGroup).withKeyGroupAssigner(new DefaultKeyGroupAssigner(maxPara));
        desc.withGraphMeta(new GraphMeta(tag));
        GraphState<T, T, T> graphState = StateFactory.buildGraphState(desc,
            new Configuration(conf));
        return graphState;
    }

    public void testWriteRead(boolean async) {
        Map<String, String> conf = new HashMap<>(config);
        conf.put(StateConfigKeys.STATE_WRITE_ASYNC_ENABLE.getKey(), Boolean.toString(async));
        GraphState<String, String, String> graphState = getGraphState(StringType.INSTANCE,
            "write_read", conf);

        // set chk = 1
        graphState.manage().operate().setCheckpointId(1L);
        // write 1 vertex and 100 edges.
        graphState.staticGraph().V().add(new ValueVertex<>("1", "vertex_hello"));
        for (int i = 0; i < 100; i++) {
            String id = Integer.toString(i);
            graphState.staticGraph().E().add(new ValueEdge<>("1", id, "edge_hello"));
        }
        // read nothing since not committed
        if (!async) {
            Assert.assertNull(graphState.staticGraph().V().query("1").get());
            Assert.assertEquals(graphState.staticGraph().E().query("1").asList().size(), 0);
        }
        // commit chk = 1, now be able to read data
        graphState.manage().operate().archive();
        Assert.assertNotNull(graphState.staticGraph().V().query("1").get());
        Assert.assertEquals(graphState.staticGraph().E().query("1").asList().size(), 100);

        // set chk = 2
        graphState.manage().operate().setCheckpointId(2L);
        // write 1 new vertex and 390 new edges.
        graphState.staticGraph().V().add(new ValueVertex<>("2", "vertex_hello"));
        for (int i = 0; i < 200; i++) {
            String id = Integer.toString(i);
            graphState.staticGraph().E().add(new ValueEdge<>("2", id, "edge_hello"));
        }
        // be not able to read data with chk = 2 since not committed.
        Assert.assertNotNull(graphState.staticGraph().V().query("1").get());
        Assert.assertEquals(graphState.staticGraph().E().query("1").asList().size(), 100);
        if (!async) {
            Assert.assertNull(graphState.staticGraph().V().query("2").get());
            Assert.assertEquals(graphState.staticGraph().E().query("2").asList().size(), 0);
        }
        // commit chk = 2, now be able to read data
        graphState.manage().operate().archive();
        Assert.assertNotNull(graphState.staticGraph().V().query("1").get());
        Assert.assertEquals(graphState.staticGraph().E().query("1").asList().size(), 100);
        Assert.assertNotNull(graphState.staticGraph().V().query("2").get());
        Assert.assertEquals(graphState.staticGraph().E().query("2").asList().size(), 200);
        // Read data which not exists.
        Assert.assertEquals(graphState.staticGraph().E().query("3").asList().size(), 0);
        Assert.assertEquals(graphState.staticGraph().E().query("").asList().size(), 0);

        // TODO. Rollback to chk = 1, then be not able to read data with chk = 2.
        // graphState.manage().operate().setCheckpointId(1);
        // graphState.manage().operate().recover();
        // Assert.assertNotNull(graphState.staticGraph().V().query("1").get());
        // Assert.assertEquals(graphState.staticGraph().E().query("1").asList().size(), 390);
        // Assert.assertNull(graphState.staticGraph().V().query("2").get());
        // Assert.assertEquals(graphState.staticGraph().E().query("2").asList().size(), 0);

        graphState.manage().operate().close();
        graphState.manage().operate().drop();
    }

    @Test
    public void testBothWriteMode() {
        testWriteRead(true);
        testWriteRead(false);
    }

}
