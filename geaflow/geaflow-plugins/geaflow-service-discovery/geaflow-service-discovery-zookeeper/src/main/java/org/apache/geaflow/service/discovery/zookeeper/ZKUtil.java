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

import com.google.common.collect.Lists;
import java.io.IOException;
import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.common.exception.GeaflowRuntimeException;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZKUtil {

    private static final Logger LOG = LoggerFactory.getLogger(ZKUtil.class);
    public static final char ZNODE_PATH_SEPARATOR = '/';
    public static final List<ACL> CREATOR_ALL_AND_WORLD_READABLE = Lists.newArrayList(
        new ACL(ZooDefs.Perms.ALL, Ids.ANYONE_ID_UNSAFE));

    public static RecoverableZooKeeper connect(Configuration conf, Watcher watcher)
        throws IOException {
        String quorumServers = conf.getString(ZooKeeperConfigKeys.ZOOKEEPER_QUORUM_SERVERS);
        if (quorumServers == null) {
            throw new IllegalArgumentException("Not find zookeeper quorumServers");
        }
        int timeout = conf.getInteger(ZooKeeperConfigKeys.ZOOKEEPER_SESSION_TIMEOUT);
        LOG.info("opening connection to ZooKeeper with quorumServers {}", quorumServers);
        int retry = conf.getInteger(ZooKeeperConfigKeys.ZOOKEEPER_RETRY);
        int retryIntervalMillis = conf.getInteger(
            ZooKeeperConfigKeys.ZOOKEEPER_RETRY_INTERVAL_MILL);
        return new RecoverableZooKeeper(quorumServers, timeout, watcher, retry,
            retryIntervalMillis);
    }


    public static String joinZNode(String prefix, String suffix) {
        if (StringUtils.isBlank(suffix)) {
            return prefix;
        }
        return prefix + ZNODE_PATH_SEPARATOR + suffix;
    }

    /**
     * Watch the specified znode for delete/create/change events.  The watcher is
     * set whether or not the node exists.  If the node already exists, the method
     * returns true.  If the node does not exist, the method returns false.
     */
    public static boolean watchAndCheckExists(ZooKeeperWatcher zkw, String znode) {
        try {
            Stat s = zkw.getRecoverableZooKeeper().exists(znode, zkw);
            boolean exists = s != null;
            if (exists) {
                LOG.info("Set watcher on existing znode {}", znode);
            } else {
                LOG.info("{} does not exist. Watcher is set.", znode);
            }
            return exists;
        } catch (Exception e) {
            LOG.warn("Unable to set watcher on znode {}", znode, e);
            return false;
        }
    }

    /**
     * Check if the specified node exists.  Sets no watches.
     */
    public static boolean exists(ZooKeeperWatcher zkw, String znode) {
        try {
            return zkw.getRecoverableZooKeeper().exists(znode, null) != null;
        } catch (Exception e) {
            LOG.warn("Unable to set watcher on znode ({})", znode, e);
            return false;
        }
    }

    public static byte[] getDataAndWatch(ZooKeeperWatcher zkw, String znode) {
        return getDataInternal(zkw, znode, null);
    }

    private static byte[] getDataInternal(ZooKeeperWatcher zkw, String znode, Stat stat) {
        try {
            return zkw.getRecoverableZooKeeper().getData(znode, zkw, stat);
        } catch (Exception e) {
            throw new GeaflowRuntimeException(e);
        }
    }


    public static boolean createEphemeralNodeAndWatch(ZooKeeperWatcher zkw, String znode,
                                                      byte[] data) {
        try {
            zkw.getRecoverableZooKeeper()
                .create(znode, data, CREATOR_ALL_AND_WORLD_READABLE, CreateMode.EPHEMERAL);
        } catch (KeeperException.NodeExistsException nee) {
            if (!watchAndCheckExists(zkw, znode)) {
                // It did exist but now it doesn't, try again
                return createEphemeralNodeAndWatch(zkw, znode, data);
            }
            return false;
        } catch (Exception e) {
            throw new GeaflowRuntimeException(e);
        }
        return true;
    }

    public static void deleteNode(ZooKeeperWatcher zkw, String node) {
        zkw.getRecoverableZooKeeper().delete(node, -1);
    }

    public static void createPersistentNode(ZooKeeperWatcher zkw, String znode) {

        RecoverableZooKeeper zk = zkw.getRecoverableZooKeeper();
        try {
            Stat stat = zk.exists(znode, false);
            if (stat == null) {
                String path = zk.create(znode, new byte[0], CREATOR_ALL_AND_WORLD_READABLE,
                    CreateMode.PERSISTENT);
                LOG.info("{} create {} success", path, CreateMode.PERSISTENT);
            } else {
                LOG.info("{} exits, skip create", znode);
            }
        } catch (Exception e) {
            throw new GeaflowRuntimeException(e);
        }
    }

    public static boolean updatePersistentNode(ZooKeeperWatcher zkw, String znode, byte[] data) {
        RecoverableZooKeeper zk = zkw.getRecoverableZooKeeper();
        try {
            if (zk.exists(znode, false) == null) {
                String path = zk.create(znode, data, CREATOR_ALL_AND_WORLD_READABLE,
                    CreateMode.PERSISTENT);
                LOG.info("{} create success", path);
                return true;
            }
            zk.setData(znode, data);
        } catch (Exception e) {
            throw new GeaflowRuntimeException(e);
        }
        return true;
    }

}
