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

package org.apache.geaflow.service.discovery.zookeeper;

import com.google.common.primitives.Longs;
import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.curator.test.TestingServer;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.common.config.keys.ExecutionConfigKeys;
import org.apache.geaflow.common.utils.PortUtil;
import org.apache.geaflow.service.discovery.ServiceBuilderFactory;
import org.apache.geaflow.service.discovery.ServiceConsumer;
import org.apache.geaflow.service.discovery.ServiceListener;
import org.apache.zookeeper.KeeperException;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class ZooKeeperTest {

    private static final String MASTER = "master";

    private Configuration configuration;

    private ZooKeeperServiceProvider serviceProvider;

    private TestListener listener;

    private String serviceType = "zookeeper";

    private String baseNode;

    private File testDir;

    private TestingServer server;

    @BeforeMethod
    public void before() throws Exception {
        testDir = new File("/tmp/zk" + System.currentTimeMillis());
        if (!testDir.exists()) {
            testDir.mkdir();
        }
        int port = PortUtil.getPort(5000, 6000);
        server = new TestingServer(port, testDir);
        server.start();

        baseNode = "/test_zk" + System.currentTimeMillis();
        configuration = new Configuration();
        configuration.put(ZooKeeperConfigKeys.ZOOKEEPER_BASE_NODE, baseNode);
        configuration.put(ZooKeeperConfigKeys.ZOOKEEPER_QUORUM_SERVERS, "localhost:" + port);
    }

    @AfterMethod
    public void tearDown() throws IOException {
        server.stop();
        testDir.delete();
    }

    @Test
    public void testCreateBaseNode() {
        serviceProvider = (ZooKeeperServiceProvider) ServiceBuilderFactory.build(serviceType)
            .buildProvider(configuration);

        Assert.assertTrue(serviceProvider.exists(""));
    }

    @Test
    public void testCreateSequential() throws KeeperException {
        serviceProvider = (ZooKeeperServiceProvider) ServiceBuilderFactory.build(serviceType)
            .buildProvider(configuration);
        listener = new TestListener();
        serviceProvider.register(listener);

        serviceProvider.watchAndCheckExists(MASTER);

        String data = "123";
        boolean res = serviceProvider.createAndWatch(MASTER, data.getBytes());
        Assert.assertTrue(res);
        listener.updatePath = null;

        Assert.assertTrue(serviceProvider.exists(MASTER));

        byte[] datas = serviceProvider.getDataAndWatch(MASTER);
        Assert.assertEquals(datas, data.getBytes());

        res = serviceProvider.createAndWatch(MASTER, "data".getBytes());

        Assert.assertFalse(res);

        serviceProvider.delete(MASTER);

        res = serviceProvider.createAndWatch(MASTER, "data".getBytes());

        Assert.assertTrue(res);

        serviceProvider.delete(MASTER);
    }

    @Test
    public void testDelete() throws KeeperException {

        serviceProvider = (ZooKeeperServiceProvider) ServiceBuilderFactory.build(serviceType)
            .buildProvider(configuration);
        listener = new TestListener();
        serviceProvider.register(listener);

        boolean res = serviceProvider.createAndWatch(MASTER, "123".getBytes());

        Assert.assertTrue(res);

        Assert.assertTrue(serviceProvider.exists(MASTER));

        serviceProvider.delete(MASTER);

        Assert.assertFalse(serviceProvider.exists(MASTER));

        serviceProvider.close();
    }

    @Test
    public void testConsumer() {

        serviceProvider = (ZooKeeperServiceProvider) ServiceBuilderFactory.build(serviceType)
            .buildProvider(configuration);

        ServiceConsumer consumer = ServiceBuilderFactory.build(serviceType)
            .buildConsumer(configuration);

        listener = new TestListener();
        consumer.register(listener);

        boolean res = serviceProvider.createAndWatch(MASTER, "123".getBytes());
        Assert.assertTrue(res);

        byte[] datas = consumer.getDataAndWatch(MASTER);

        Assert.assertEquals(datas, "123".getBytes());

        serviceProvider.delete(MASTER);
    }

    @Test
    public void testUpdate() {
        String version = "version";
        serviceProvider = (ZooKeeperServiceProvider) ServiceBuilderFactory.build(serviceType)
            .buildProvider(configuration);

        ServiceConsumer consumer = ServiceBuilderFactory.build(serviceType)
            .buildConsumer(configuration);
        listener = new TestListener();
        consumer.register(listener);

        Assert.assertFalse(consumer.exists(version));
        byte[] versionData = Longs.toByteArray(2);

        serviceProvider.update(version, versionData);

        byte[] data = consumer.getDataAndWatch(version);

        Assert.assertEquals(versionData, data);

        versionData = Longs.toByteArray(3);
        serviceProvider.update(version, versionData);

        data = consumer.getDataAndWatch(version);
        Assert.assertEquals(versionData, data);

        consumer.close();
    }

    @Test
    public void testBaseNode() {

        Map<String, String> config = configuration.getConfigMap();
        Configuration newConfig = new Configuration(new HashMap<>(config));
        newConfig.getConfigMap().remove(ZooKeeperConfigKeys.ZOOKEEPER_BASE_NODE.getKey());
        newConfig.put(ExecutionConfigKeys.JOB_APP_NAME, "234");

        serviceProvider = (ZooKeeperServiceProvider) ServiceBuilderFactory.build(serviceType)
            .buildProvider(newConfig);

        Assert.assertTrue(serviceProvider.exists(null));
        serviceProvider.close();
        ServiceBuilderFactory.clear();
    }


    static class TestListener implements ServiceListener {

        private String updatePath;

        @Override
        public void nodeCreated(String path) {
            updatePath = path;
        }

        @Override
        public void nodeDeleted(String path) {
            updatePath = path;
        }

        @Override
        public void nodeDataChanged(String path) {
            updatePath = path;
        }

        @Override
        public void nodeChildrenChanged(String path) {
            updatePath = path;
        }
    }


}
