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

package org.apache.geaflow.cluster.runner.util;

import static org.apache.geaflow.cluster.constants.ClusterConstants.CONFIG_FILE_LOG4J_NAME;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertTrue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import org.apache.geaflow.cluster.config.ClusterJvmOptions;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.utils.JsonUtils;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;

public class ClusterUtilsTest {

    @Test
    public void testConfiguration() {
        Map<String, String> map = new HashMap<>();
        map.put("k", "v");
        String s = JsonUtils.toJsonString(map);
        Configuration configuration = ClusterUtils.convertStringToConfig(s);
        Assert.assertNotNull(configuration);
    }

    @Test
    public void getStartCommand_WithValidInputs_ShouldReturnCorrectCommand() {
        ClusterJvmOptions jvmOptions = Mockito.mock(ClusterJvmOptions.class);
        Configuration configuration = new Configuration();

        // 准备
        when(jvmOptions.getXmsMB()).thenReturn(1024);
        when(jvmOptions.getMaxHeapMB()).thenReturn(2048);
        when(jvmOptions.getXmnMB()).thenReturn(512);
        when(jvmOptions.getMaxDirectMB()).thenReturn(1024);
        when(jvmOptions.getExtraOptions()).thenReturn(new ArrayList<>());

        Map<String, String> extraOpts = new HashMap<>();
        extraOpts.put("key1", "value1");
        extraOpts.put("key2", "value2");

        String classpath = "/path/to/classes";
        String logFilename = "log.txt";
        Class<?> mainClass = ClusterUtilsTest.class;

        String command = ClusterUtils.getStartCommand(jvmOptions, mainClass, logFilename,
            configuration, extraOpts, classpath, true);

        // 验证
        assertTrue(command.contains("-Xms1024m"));
        assertTrue(command.contains("-Xmx2048m"));
        assertTrue(command.contains("-Xmn512m"));
        assertTrue(command.contains("-XX:MaxDirectMemorySize=1024m"));
        assertTrue(command.contains("-Dkey1=\"value1\""));
        assertTrue(command.contains("-Dkey2=\"value2\""));
        assertTrue(command.contains("-Dlog.file=log.txt"));
        assertTrue(command.contains(
            "-Dlog4j.configuration=file:/etc/geaflow/conf/" + CONFIG_FILE_LOG4J_NAME));
        assertTrue(command.contains(">> log.txt 2>&1"));
    }

}
