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

package org.apache.geaflow.cluster.k8s.utils;

import static org.apache.geaflow.cluster.k8s.config.KubernetesConfigKeys.SERVICE_EXPOSED_TYPE;
import static org.apache.geaflow.common.config.keys.ExecutionConfigKeys.CLUSTER_CLIENT_TIMEOUT_MS;
import static org.apache.geaflow.common.config.keys.ExecutionConfigKeys.REPORTER_LIST;

import io.fabric8.kubernetes.api.model.NodeSelectorRequirement;
import io.fabric8.kubernetes.api.model.Toleration;
import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.geaflow.cluster.constants.ClusterConstants;
import org.apache.geaflow.cluster.k8s.config.KubernetesConfigKeys;
import org.apache.geaflow.cluster.rpc.ConnectAddress;
import org.apache.geaflow.cluster.runner.util.ClusterUtils;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.common.utils.FileUtil;
import org.testng.Assert;
import org.testng.annotations.Test;

public class KubernetesUtilsTest {

    @Test
    public void testGetEnvTest() {
        Map<String, String> env = System.getenv();
        try {
            ClusterUtils.getEnvValue(env, "envTestKey");
        } catch (IllegalArgumentException e) {
            Assert.assertTrue(e.getMessage().contains("envTestKey is not set"));
        }
    }

    @Test
    public void testLoadConfigFromFile() {
        String path =
            this.getClass().getClassLoader().getResource("geaflow-conf-test.yml").getPath();
        Configuration config = KubernetesUtils.loadYAMLResource(new File(path));
        Assert.assertNotNull(config);
        Assert.assertEquals(config.getString(REPORTER_LIST), "slf4j");
        Assert.assertEquals(config.getString(SERVICE_EXPOSED_TYPE), "NODE_PORT");
        Assert.assertEquals(config.getInteger(CLUSTER_CLIENT_TIMEOUT_MS), 300000);
    }

    @Test
    public void testGetContentFromFile() {
        String path =
            this.getClass().getClassLoader().getResource("geaflow-conf-test.yml").getPath();
        String content = FileUtil.getContentFromFile(path);
        Assert.assertNotNull(content);
    }

    @Test
    public void testTolerances() {
        Configuration configuration = new Configuration();
        List<Toleration> tolerationList = KubernetesUtils.getTolerations(configuration);
        Assert.assertEquals(tolerationList.size(), 0);

        configuration.put(KubernetesConfigKeys.TOLERATION_LIST, "");
        tolerationList = KubernetesUtils.getTolerations(configuration);
        Assert.assertEquals(tolerationList.size(), 0);

        configuration.put(KubernetesConfigKeys.TOLERATION_LIST, "key1:Equal:value1:NoSchedule:-,key2:Exists:-:-:-");
        tolerationList = KubernetesUtils.getTolerations(configuration);
        Assert.assertEquals(tolerationList.size(), 2);
    }

    @Test
    public void testMatchExpressions() {
        Configuration configuration = new Configuration();
        List<NodeSelectorRequirement> matchExpressions = KubernetesUtils.getMatchExpressions(configuration);
        Assert.assertEquals(matchExpressions.size(), 0);

        configuration.put(KubernetesConfigKeys.MATCH_EXPRESSION_LIST, "");
        matchExpressions = KubernetesUtils.getMatchExpressions(configuration);
        Assert.assertEquals(matchExpressions.size(), 0);

        configuration.put(KubernetesConfigKeys.MATCH_EXPRESSION_LIST, "key1:In:value1,key2:In:-");
        matchExpressions = KubernetesUtils.getMatchExpressions(configuration);
        Assert.assertEquals(matchExpressions.size(), 2);
    }

    @Test
    public void testAddressEncoding() {
        Map<String, ConnectAddress> map = new HashMap<>();
        for (int i = 0; i < 3; i++) {
            map.put(ClusterConstants.getDriverName(i), new ConnectAddress("127.0.0.1", 80));
        }
        String encodedStr = KubernetesUtils.encodeRpcAddressMap(map);
        Map<String, ConnectAddress> map2 = KubernetesUtils.decodeRpcAddressMap(encodedStr);
        Assert.assertEquals(map, map2);
    }
}
