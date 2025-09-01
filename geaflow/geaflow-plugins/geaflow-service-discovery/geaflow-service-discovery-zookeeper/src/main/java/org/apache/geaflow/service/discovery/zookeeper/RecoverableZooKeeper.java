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

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import org.apache.geaflow.common.utils.ProcessUtil;
import org.apache.geaflow.common.utils.RetryCommand;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RecoverableZooKeeper {

    private static final Logger LOGGER = LoggerFactory.getLogger(RecoverableZooKeeper.class);
    private final ZooKeeper zk;
    private final int retryIntervalMillis;
    private final int maxRetries;
    private final String identifier;

    public RecoverableZooKeeper(String quorumServers, int sessionTimeout, Watcher watcher,
                                int maxRetries, int retryIntervalMillis) throws IOException {
        this.zk = new ZooKeeper(quorumServers, sessionTimeout, watcher);
        this.retryIntervalMillis = retryIntervalMillis;
        this.identifier = ProcessUtil.getHostAndPid();
        LOGGER.info("The identifier of this process is {}", identifier);
        this.maxRetries = maxRetries;
    }


    public boolean delete(String path, int version) {
        return Boolean.TRUE.equals(RetryCommand.run(() -> {
            zk.delete(path, version);
            return true;
        }, maxRetries, retryIntervalMillis));
    }


    public Stat exists(String path, Watcher watcher) {
        return RetryCommand.run(() -> zk.exists(path, watcher), maxRetries, retryIntervalMillis);
    }


    public Stat exists(String path, boolean watch) {
        return RetryCommand.run(() -> zk.exists(path, watch), maxRetries, retryIntervalMillis);
    }

    public List<String> getChildren(String path, Watcher watcher) {
        return RetryCommand.run(() -> zk.getChildren(path, watcher), maxRetries,
            retryIntervalMillis);
    }

    public byte[] getData(String path, Watcher watcher, Stat stat) {
        return RetryCommand.run(() -> zk.getData(path, watcher, stat), maxRetries,
            retryIntervalMillis);
    }

    public String create(String path, byte[] data, List<ACL> acl, CreateMode createMode)
        throws KeeperException, InterruptedException {

        switch (createMode) {
            case EPHEMERAL:
            case PERSISTENT:
                return createNonSequential(path, data, acl, createMode);
            default:
                throw new IllegalArgumentException("Unrecognized CreateMode: " + createMode);
        }
    }

    private String createNonSequential(String path, byte[] data, List<ACL> acl,
                                       CreateMode createMode)
        throws KeeperException, InterruptedException {
        if (exists(path, false) != null) {
            byte[] currentData = zk.getData(path, false, null);
            if (currentData != null && Arrays.equals(currentData, data)) {
                // We successfully created a non-sequential node
                return path;
            }
        }
        try {
            return zk.create(path, data, acl, createMode);
        } catch (KeeperException e) {
            if (Objects.requireNonNull(e.code()) == Code.NODEEXISTS) {
                // If the connection was lost, there is still a possibility that
                // we have successfully created the node at our previous attempt,
                // so we read the node and compare.
                if (exists(path, false) != null) {
                    byte[] currentData = zk.getData(path, false, null);
                    if (currentData != null && Arrays.equals(currentData, data)) {
                        // We successfully created a non-sequential node
                        return path;
                    }
                    LOGGER.error("Node {} already exists with {}, could not write {}", path,
                        Arrays.toString(currentData), Arrays.toString(data));
                }
            }
            throw e;
        }
    }

    public boolean setData(String path, byte[] data) throws KeeperException, InterruptedException {
        try {
            if (exists(path, false) != null) {
                byte[] currentData = zk.getData(path, false, null);
                if (currentData != null && Arrays.equals(currentData, data)) {
                    return true;
                }
            }
            zk.setData(path, data, -1);
        } catch (KeeperException e) {
            throw e;
        }
        return true;
    }

    public long getSessionId() {
        return zk.getSessionId();
    }

    public void close() throws InterruptedException {
        zk.close();
    }
}
