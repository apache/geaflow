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

package org.apache.geaflow.store.lmdb;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.common.config.keys.ExecutionConfigKeys;
import org.apache.geaflow.common.config.keys.FrameworkConfigKeys;
import org.apache.geaflow.common.config.keys.StateConfigKeys;
import org.apache.geaflow.common.errorcode.RuntimeErrors;
import org.apache.geaflow.common.exception.GeaflowRuntimeException;
import org.apache.geaflow.file.FileConfigKeys;
import org.apache.geaflow.store.IStatefulStore;
import org.apache.geaflow.store.api.graph.BaseGraphStore;
import org.apache.geaflow.store.context.StoreContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class BaseLmdbStore extends BaseGraphStore implements IStatefulStore {

    private static final Logger LOGGER = LoggerFactory.getLogger(BaseLmdbStore.class);

    protected Configuration config;
    protected String lmdbPath;
    protected String remotePath;
    protected LmdbClient lmdbClient;
    protected LmdbPersistClient persistClient;
    protected long keepChkNum;

    protected String root;
    protected String jobName;
    protected int shardId;
    protected long recoveryVersion = -1;

    @Override
    public void init(StoreContext storeContext) {
        super.init(storeContext);
        this.config = storeContext.getConfig();
        this.shardId = storeContext.getShardId();

        String workerPath = this.config.getString(ExecutionConfigKeys.JOB_WORK_PATH);
        this.jobName = this.config.getString(ExecutionConfigKeys.JOB_APP_NAME);

        this.lmdbPath = Paths.get(workerPath, jobName, storeContext.getName(),
            Integer.toString(shardId)).toString();

        this.root = this.config.getString(FileConfigKeys.ROOT);

        this.remotePath = getRemotePath().toString();
        this.persistClient = new LmdbPersistClient(this.config);
        long chkRate = this.config.getLong(FrameworkConfigKeys.BATCH_NUMBER_PER_CHECKPOINT);
        this.keepChkNum = Math.max(
            this.config.getInteger(StateConfigKeys.STATE_ARCHIVED_VERSION_NUM), chkRate * 2);

        boolean enableDynamicCreateDatabase = PartitionType.getEnum(
                this.config.getString(LmdbConfigKeys.LMDB_GRAPH_STORE_PARTITION_TYPE))
            .isPartition();
        this.lmdbClient = new LmdbClient(lmdbPath, getDbList(), config,
            enableDynamicCreateDatabase);
        LOGGER.info("ThreadId {}, BaseLmdbStore initDB", Thread.currentThread().getId());
        this.lmdbClient.initDB();
    }

    protected abstract List<String> getDbList();

    @Override
    public void archive(long version) {
        flush();
        String chkPath = LmdbConfigKeys.getChkPath(this.lmdbPath, version);
        lmdbClient.checkpoint(chkPath);
        // sync file
        try {
            persistClient.archive(version, chkPath, remotePath, keepChkNum);
        } catch (Exception e) {
            throw new GeaflowRuntimeException(RuntimeErrors.INST.runError("archive fail"), e);
        }
    }

    @Override
    public void recovery(long version) {
        if (version <= recoveryVersion) {
            LOGGER.info("shardId {} recovery version {} <= last recovery version {}, ignore",
                shardId, version, recoveryVersion);
            return;
        }
        drop();
        String chkPath = LmdbConfigKeys.getChkPath(this.lmdbPath, version);
        String recoverPath = remotePath;
        boolean isScale = shardId != storeContext.getShardId();
        if (isScale) {
            recoverPath = getRemotePath().toString();
        }
        try {
            persistClient.recover(version, this.lmdbPath, chkPath, recoverPath);
        } catch (Exception e) {
            throw new GeaflowRuntimeException(RuntimeErrors.INST.runError("recover fail"), e);
        }
        if (isScale) {
            persistClient.clearFileInfo();
            shardId = storeContext.getShardId();
        }
        this.lmdbClient.initDB();
        recoveryVersion = version;
    }

    protected Path getRemotePath() {
        return Paths.get(root, jobName, storeContext.getName(), Integer.toString(shardId));
    }

    @Override
    public long recoveryLatest() {
        long chkId = persistClient.getLatestCheckpointId(remotePath);
        if (chkId > 0) {
            recovery(chkId);
        }
        return chkId;
    }

    @Override
    public void compact() {
        // LMDB doesn't need compaction (B+tree structure)
        // This is a no-op for compatibility
        this.lmdbClient.compact();
    }

    @Override
    public void flush() {
        this.lmdbClient.flush();
    }

    @Override
    public void close() {
        this.lmdbClient.close();
    }

    @Override
    public void drop() {
        lmdbClient.drop();
    }

    /**
     * Get the underlying LMDB client for advanced operations.
     *
     * @return the LMDB client instance
     */
    public LmdbClient getLmdbClient() {
        return lmdbClient;
    }
}
