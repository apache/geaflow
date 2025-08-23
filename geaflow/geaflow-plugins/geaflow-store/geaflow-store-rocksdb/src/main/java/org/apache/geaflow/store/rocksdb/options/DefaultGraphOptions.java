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

package org.apache.geaflow.store.rocksdb.options;

import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.store.rocksdb.RocksdbConfigKeys;
import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.BloomFilter;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.FlushOptions;
import org.rocksdb.Options;
import org.rocksdb.ReadOptions;
import org.rocksdb.Statistics;
import org.rocksdb.StatsLevel;
import org.rocksdb.StringAppendOperator;
import org.rocksdb.WriteOptions;
import org.rocksdb.util.SizeUnit;

public class DefaultGraphOptions implements IRocksDBOptions {

    protected Statistics statistics;

    protected Options options = new Options();

    protected DBOptions dbOptions = new DBOptions();

    protected WriteOptions writeOptions = new WriteOptions();

    protected ReadOptions readOptions = new ReadOptions();

    protected FlushOptions flushOptions = new FlushOptions();

    protected boolean closed;
    private int maxWriteBufferNumber;
    private long blockSize;
    private long blockCacheSize;
    private long writeBufferSize;
    private long targetFileSize;

    public DefaultGraphOptions() {

    }

    protected void initOption() {
        options.setUseDirectIoForFlushAndCompaction(true);
        options.setCreateIfMissing(true);
        options.setCreateMissingColumnFamilies(true);
        options.setMergeOperator(new StringAppendOperator());
        options.setTableFormatConfig(buildBlockBasedTableConfig());
        options.setMaxWriteBufferNumber(maxWriteBufferNumber);
        // Amount of data to build up in memory (backed by an unsorted log
        // on disk) before converting to a sorted on-disk file. 64MB default
        options.setWriteBufferSize(writeBufferSize);
        // target_file_size_base is per-file size for level-1.
        options.setTargetFileSizeBase(targetFileSize);
        options.setMaxBackgroundFlushes(2);
        options.setMaxBackgroundCompactions(2);
        options.setLevelZeroFileNumCompactionTrigger(20);
        // Soft limit on number of level-0 files.
        options.setLevelZeroSlowdownWritesTrigger(30);
        // Maximum number of level-0 files.  We stop writes at this point.
        options.setLevelZeroStopWritesTrigger(40);
        options.setNumLevels(4);
        options.setMaxManifestFileSize(50 * SizeUnit.KB);

        dbOptions.setCreateIfMissing(true);
        dbOptions.setCreateMissingColumnFamilies(true);
        dbOptions.setMaxBackgroundFlushes(2);
        dbOptions.setMaxBackgroundCompactions(6);
        dbOptions.setMaxManifestFileSize(50 * SizeUnit.KB);

        writeOptions.setDisableWAL(true);
        flushOptions.setWaitForFlush(true);
    }

    @Override
    public void init(Configuration config) {
        this.maxWriteBufferNumber =
            config.getInteger(RocksdbConfigKeys.ROCKSDB_OPTIONS_MAX_WRITER_BUFFER_NUM);
        this.writeBufferSize = config.getLong(RocksdbConfigKeys.ROCKSDB_OPTIONS_WRITER_BUFFER_SIZE);
        this.targetFileSize = config.getLong(RocksdbConfigKeys.ROCKSDB_OPTIONS_TARGET_FILE_SIZE);
        this.blockSize = config.getLong(RocksdbConfigKeys.ROCKSDB_OPTIONS_TABLE_BLOCK_SIZE);
        this.blockCacheSize = config.getLong(RocksdbConfigKeys.ROCKSDB_OPTIONS_TABLE_BLOCK_CACHE_SIZE);

        initOption();
    }

    public BlockBasedTableConfig buildBlockBasedTableConfig() {
        BlockBasedTableConfig tableOptions = new BlockBasedTableConfig();
        tableOptions.setBlockSize(blockSize);
        tableOptions.setBlockCacheSize(blockCacheSize);
        tableOptions.setFilter(new BloomFilter(10, false));
        tableOptions.setCacheIndexAndFilterBlocks(true);
        tableOptions.setPinL0FilterAndIndexBlocksInCache(true);
        return tableOptions;
    }

    public Options getOptions() {
        return options;
    }

    public WriteOptions getWriteOptions() {
        return writeOptions;
    }

    public ReadOptions getReadOptions() {
        return readOptions;
    }

    public FlushOptions getFlushOptions() {
        return flushOptions;
    }

    @Override
    public DBOptions getDbOptions() {
        return dbOptions;
    }

    @Override
    public ColumnFamilyOptions buildFamilyOptions() {
        ColumnFamilyOptions columnFamilyOptions = new ColumnFamilyOptions();
        columnFamilyOptions.setWriteBufferSize(writeBufferSize);
        // target_file_size_base is per-file size for level-1.
        columnFamilyOptions.setTargetFileSizeBase(targetFileSize);
        columnFamilyOptions.setLevelZeroFileNumCompactionTrigger(20);
        // Soft limit on number of level-0 files.
        columnFamilyOptions.setLevelZeroSlowdownWritesTrigger(30);
        // Maximum number of level-0 files.  We stop writes at this point.
        columnFamilyOptions.setLevelZeroStopWritesTrigger(40);

        BlockBasedTableConfig tableConfig = buildBlockBasedTableConfig();
        tableConfig.setBlockSize(blockSize);
        tableConfig.setBlockCacheSize(blockCacheSize);
        columnFamilyOptions.setTableFormatConfig(tableConfig);
        columnFamilyOptions.setMaxWriteBufferNumber(2);

        return columnFamilyOptions;
    }

    @Override
    public Statistics getStatistics() {
        return statistics;
    }

    @Override
    public void enableParanoidCheck() {
        options.setParanoidChecks(true);
    }

    @Override
    public void enableStatistics() {
        statistics = new Statistics();
        statistics.setStatsLevel(StatsLevel.ALL);
        options.setStatistics(this.statistics);
    }

    public void close() {
        this.options.close();
        if (statistics != null) {
            statistics.close();
        }
        this.closed = true;
    }

    public boolean isClosed() {
        return closed;
    }
}
