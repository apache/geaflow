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

package org.apache.geaflow.metaserver;

import java.io.File;
import java.io.IOException;
import java.util.List;
import org.apache.curator.test.TestingServer;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.common.config.keys.ExecutionConfigKeys;
import org.apache.geaflow.common.rpc.HostAndPort;
import org.apache.geaflow.common.utils.FileUtil;
import org.apache.geaflow.metaserver.client.MetaServerQueryClient;
import org.apache.geaflow.metaserver.internal.MetaServerClient;
import org.apache.geaflow.metaserver.service.NamespaceType;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

public class ZookeeperMetaServerTest {
    private TestingServer zkServer;
    private File testDir;
    private MetaServer metaServer;
    private final Configuration configuration = new Configuration();

    private void before(Configuration configuration) throws Exception {
        String jobName = "test_zookeeper" + System.currentTimeMillis();
        testDir = new File(FileUtil.constitutePath("tmp", "zk", jobName));
        configuration.put("geaflow.zookeeper.znode.parent", File.separator + jobName);
        configuration.put("geaflow.zookeeper.quorum.servers", "localhost:2181");
        if (testDir.exists()) {
            testDir.delete();
        }
        if (!testDir.exists()) {
            testDir.mkdir();
        }
        zkServer = new TestingServer(2181, testDir);
        zkServer.start();
        metaServer = new MetaServer();
        metaServer.init(new MetaServerContext(configuration));
    }

    @AfterClass
    private void after() throws IOException {
        if (metaServer != null) {
            metaServer.close();
        }
        if (zkServer != null) {
            zkServer.stop();
            testDir.delete();
        }
    }


    @Test
    public void testZookeeperRegister() throws Exception {
        this.configuration.put(ExecutionConfigKeys.SERVICE_DISCOVERY_TYPE, "zookeeper");
        before(this.configuration);
        MetaServerClient client = MetaServerClient.getClient(configuration);

        client.registerService(NamespaceType.DEFAULT, "1", new HostAndPort("127.0.0.1", 1000));
        client.registerService(NamespaceType.DEFAULT, "2", new HostAndPort("127.0.0.1", 10242));
    }

    @Test(dependsOnMethods = "testZookeeperRegister")
    public void testZookeeperQueryService() {
        this.configuration.put(ExecutionConfigKeys.SERVICE_DISCOVERY_TYPE, "zookeeper");
        MetaServerQueryClient queryClient = MetaServerQueryClient.getClient(configuration);
        List<HostAndPort> serviceInfos = queryClient.queryAllServices(NamespaceType.DEFAULT);

        Assert.assertEquals(serviceInfos.size(), 2);
        Assert.assertTrue(serviceInfos.contains(new HostAndPort("127.0.0.1", 1000)));
        Assert.assertTrue(serviceInfos.contains(new HostAndPort("127.0.0.1", 10242)));
    }
}
