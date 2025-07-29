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
import org.apache.geaflow.file.FileConfigKeys;
import org.apache.geaflow.state.descriptor.KeyMapStateDescriptor;
import org.apache.geaflow.store.paimon.PaimonConfigKeys;
import org.apache.geaflow.utils.keygroup.DefaultKeyGroupAssigner;
import org.apache.geaflow.utils.keygroup.KeyGroup;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class PaimonKeyStateTest {

    static Map<String, String> config = new HashMap<>();

    @BeforeClass
    public static void setUp() {
        FileUtils.deleteQuietly(new File("/tmp/geaflow/chk/"));
        FileUtils.deleteQuietly(new File("/tmp/PaimonKeyStateTest/"));
        config.put(ExecutionConfigKeys.JOB_APP_NAME.getKey(), "PaimonKeyStateTest");
        config.put(FileConfigKeys.PERSISTENT_TYPE.getKey(), "LOCAL");
        config.put(FileConfigKeys.ROOT.getKey(), "/tmp/geaflow/chk/");
        config.put(PaimonConfigKeys.PAIMON_OPTIONS_WAREHOUSE.getKey(), "file:///tmp/PaimonKeyStateTest/");
    }

    @AfterClass
    public static void tearUp() {
        FileUtils.deleteQuietly(new File("/tmp/PaimonKeyStateTest"));
    }

    @Test
    public void testKMap() {
        KeyMapStateDescriptor<String, String, String> desc =
            KeyMapStateDescriptor.build("testKV", StoreType.PAIMON.name());
        desc.withKeyGroup(new KeyGroup(0, 0))
            .withKeyGroupAssigner(new DefaultKeyGroupAssigner(1));
        KeyMapState<String, String, String> mapState = StateFactory.buildKeyMapState(desc,
            new Configuration(config));

        // set chk = 1
        mapState.manage().operate().setCheckpointId(1L);
        // write data
        Map<String, String> conf = new HashMap<>(config);
        mapState.put("hello", conf);
        // read nothing since not committed.
        Assert.assertEquals(mapState.get("hello").size(), 0);
        // commit chk = 1, now be able to read data.
        mapState.manage().operate().archive();
        Assert.assertEquals(mapState.get("hello").size(), 4);

        // set chk = 2
        mapState.manage().operate().setCheckpointId(2L);

        Map<String, String> conf2 = new HashMap<>(config);
        conf2.put("conf2", "test");
        mapState.put("hello2", conf2);
        // cannot read data with chk = 2 since chk2 not committed.
        Assert.assertEquals(mapState.get("hello").size(), 4);
        Assert.assertEquals(mapState.get("hello2").size(), 0);

        // commit chk = 2
        mapState.manage().operate().finish();
        mapState.manage().operate().archive();

        // now be able to read data
        Assert.assertEquals(mapState.get("hello").size(), 4);
        Assert.assertEquals(mapState.get("hello2").size(), 5);

        // read data which not exists
        Assert.assertEquals(mapState.get("hello3").size(), 0);

        // TODO. recover to chk = 1, then be not able to read data with chk = 2.
        // mapState = StateFactory.buildKeyMapState(desc,
        //     new Configuration(config));
        // mapState.manage().operate().setCheckpointId(1L);
        // mapState.manage().operate().recover();
        // Assert.assertEquals(mapState.get("hello").size(), 4);
        // Assert.assertEquals(mapState.get("hell2").size(), 0);


        mapState.manage().operate().close();
        mapState.manage().operate().drop();
    }

}
