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

package org.apache.geaflow.cluster.container;

import java.io.File;
import java.util.Objects;
import org.apache.commons.io.FileUtils;
import org.apache.geaflow.cluster.protocol.EventType;
import org.apache.geaflow.cluster.protocol.IEvent;
import org.apache.geaflow.cluster.protocol.IHighAvailableEvent;
import org.apache.geaflow.cluster.system.ClusterMetaStore;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.common.config.keys.ExecutionConfigKeys;
import org.apache.geaflow.file.FileConfigKeys;
import org.apache.geaflow.ha.runtime.HighAvailableLevel;
import org.junit.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class ContainerContextTest {

    private Configuration configuration = new Configuration();

    @BeforeMethod
    public void before() {
        String path = "/tmp/" + ContainerContextTest.class.getSimpleName();
        FileUtils.deleteQuietly(new File(path));

        configuration.getConfigMap().clear();
        configuration.put(ExecutionConfigKeys.JOB_APP_NAME.getKey(), ContainerContextTest.class.getSimpleName());
        configuration.put(FileConfigKeys.PERSISTENT_TYPE.getKey(), "LOCAL");
        configuration.put(FileConfigKeys.ROOT.getKey(), path);
        configuration.put(ExecutionConfigKeys.CLUSTER_ID, "test1");
        ClusterMetaStore.close();
    }

    @AfterMethod
    public void after() {
        String path = "/tmp/" + ContainerContextTest.class.getSimpleName();
        FileUtils.deleteQuietly(new File(path));
        ClusterMetaStore.close();
    }

    @Test
    public void testContainer() {

        int containerId = 1;
        ClusterMetaStore.init(containerId, "container-0", configuration);
        ContainerContext containerContext = new ContainerContext(containerId, configuration);

        TestHAEvent event = new TestHAEvent();
        containerContext.addEvent(event);
        containerContext.checkpoint(new ContainerContext.EventCheckpointFunction());

        // recover
        ContainerContext recoverContext = new ContainerContext(containerId, configuration, true);
        recoverContext.load();
        Assert.assertEquals(1, recoverContext.getReliableEvents().size());
        Assert.assertEquals(event, recoverContext.getReliableEvents().get(0));

        // ---- mock restart job ----
        // cluster id is changed, re-init cluster metastore.
        ClusterMetaStore.close();
        configuration.put(ExecutionConfigKeys.CLUSTER_ID, "test2");
        ClusterMetaStore.init(containerId, "container-0", configuration);
        // rebuild, context reliable event list is empty, and metastore is cleaned.
        ContainerContext restartContext = new ContainerContext(containerId, configuration);
        restartContext.load();
        Assert.assertEquals(0, restartContext.getReliableEvents().size());
        Assert.assertNull(ClusterMetaStore.getInstance().getEvents());

    }

    @Test
    public void testCheckpoint() {
        int containerId = 1;
        ClusterMetaStore.init(containerId, "container-0", configuration);
        ContainerContext containerContext = new ContainerContext(containerId, configuration);

        TestHAEvent event = new TestHAEvent("test1", 1);
        containerContext.addEvent(event);
        containerContext.checkpoint(new ContainerContext.EventCheckpointFunction());
        event.variable = 2;

        TestHAEvent event2 = new TestHAEvent("test2", 2);
        containerContext.addEvent(event2);
        containerContext.checkpoint(new ContainerContext.EventCheckpointFunction());

        ContainerContext newContext = new ContainerContext(containerId, configuration, true);
        newContext.load();
        Assert.assertEquals(2, newContext.getReliableEvents().size());
        Assert.assertEquals(1, ((TestHAEvent) (newContext.getReliableEvents().get(0))).variable);
        Assert.assertEquals(2, ((TestHAEvent) (newContext.getReliableEvents().get(1))).variable);
    }

    private class TestHAEvent implements IEvent, IHighAvailableEvent {

        private String name = "testEvent";
        private int variable = 0;

        public TestHAEvent() {
        }

        public TestHAEvent(String name, int variable) {
            this.name = name;
            this.variable = variable;
        }

        @Override
        public EventType getEventType() {
            return null;
        }

        @Override
        public HighAvailableLevel getHaLevel() {
            return HighAvailableLevel.CHECKPOINT;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            TestHAEvent that = (TestHAEvent) o;
            return Objects.equals(name, that.name);
        }

        @Override
        public int hashCode() {
            return Objects.hash(name);
        }

        @Override
        public String toString() {
            return "TestHAEvent{"
                + "name='" + name + '\''
                + ", variable=" + variable
                + '}';
        }
    }
}
