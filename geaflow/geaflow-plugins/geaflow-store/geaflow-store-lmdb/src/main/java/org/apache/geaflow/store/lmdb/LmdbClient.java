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

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.commons.io.FileUtils;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.common.errorcode.RuntimeErrors;
import org.apache.geaflow.common.exception.GeaflowRuntimeException;
import org.apache.geaflow.common.tuple.Tuple;
import org.lmdbjava.Dbi;
import org.lmdbjava.DbiFlags;
import org.lmdbjava.Env;
import org.lmdbjava.EnvFlags;
import org.lmdbjava.Txn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LmdbClient {

    private static final Logger LOGGER = LoggerFactory.getLogger(LmdbClient.class);

    private final String filePath;
    private final Configuration config;
    private final List<String> dbList;
    private Env<ByteBuffer> env;

    // database name -> database instance
    private final Map<String, Dbi<ByteBuffer>> dbiMap = new HashMap<>();
    private final Map<String, Dbi<ByteBuffer>> vertexDbiMap = new ConcurrentHashMap<>();
    private final Map<String, Dbi<ByteBuffer>> edgeDbiMap = new ConcurrentHashMap<>();

    private boolean enableDynamicCreateDatabase;
    private Txn<ByteBuffer> writeTxn;
    private final Object writeLock = new Object();
    private long flushCounter = 0;
    private static final long MAP_SIZE_CHECK_INTERVAL = 100; // Check every 100 flushes

    public LmdbClient(String filePath, List<String> dbList, Configuration config,
                      boolean enableDynamicCreateDatabase) {
        this(filePath, dbList, config);
        this.enableDynamicCreateDatabase = enableDynamicCreateDatabase;
    }

    public LmdbClient(String filePath, List<String> dbList, Configuration config) {
        this.filePath = filePath;
        this.dbList = dbList;
        this.config = config;
    }

    public void initDB() {
        File dbFile = new File(filePath);
        if (!dbFile.exists()) {
            try {
                FileUtils.forceMkdir(dbFile);
            } catch (IOException e) {
                throw new GeaflowRuntimeException(RuntimeErrors.INST.runError("create file error"), e);
            }
        }

        if (env == null) {
            LOGGER.info("ThreadId {}, buildDB {}", Thread.currentThread().getId(), filePath);

            final long mapSize = config.getLong(LmdbConfigKeys.LMDB_MAP_SIZE);
            final int maxReaders = config.getInteger(LmdbConfigKeys.LMDB_MAX_READERS);
            boolean noTls = config.getBoolean(LmdbConfigKeys.LMDB_NO_TLS);
            boolean writeMap = config.getBoolean(LmdbConfigKeys.LMDB_WRITE_MAP);

            // Build environment flags
            List<EnvFlags> envFlags = new ArrayList<>();
            if (noTls) {
                envFlags.add(EnvFlags.MDB_NOTLS);
            }
            if (writeMap) {
                envFlags.add(EnvFlags.MDB_WRITEMAP);
            }

            String syncMode = config.getString(LmdbConfigKeys.LMDB_SYNC_MODE);
            switch (syncMode.toUpperCase()) {
                case "NO_SYNC":
                    envFlags.add(EnvFlags.MDB_NOSYNC);
                    break;
                case "META_SYNC":
                    envFlags.add(EnvFlags.MDB_NOMETASYNC);
                    break;
                case "SYNC":
                default:
                    // Full sync (default)
                    break;
            }

            // Create LMDB environment
            Env.Builder<ByteBuffer> envBuilder = Env.create()
                .setMapSize(mapSize)
                .setMaxReaders(maxReaders)
                .setMaxDbs(dbList.size() + 100);  // Allow extra databases for dynamic creation

            // Open environment with flags (open method accepts EnvFlags varargs)
            if (envFlags.isEmpty()) {
                env = envBuilder.open(dbFile);
            } else {
                env = envBuilder.open(dbFile, envFlags.toArray(new EnvFlags[0]));
            }

            // Initialize databases
            for (String dbName : dbList) {
                Dbi<ByteBuffer> dbi = env.openDbi(dbName, DbiFlags.MDB_CREATE);

                if (enableDynamicCreateDatabase) {
                    if (dbName.contains(LmdbConfigKeys.VERTEX_DB_PREFIX)) {
                        vertexDbiMap.put(dbName, dbi);
                    } else if (dbName.contains(LmdbConfigKeys.EDGE_DB_PREFIX)) {
                        edgeDbiMap.put(dbName, dbi);
                    } else {
                        dbiMap.put(dbName, dbi);
                    }
                } else {
                    dbiMap.put(dbName, dbi);
                }
            }
        }
    }

    public Map<String, Dbi<ByteBuffer>> getDatabaseMap() {
        return dbiMap;
    }

    public Map<String, Dbi<ByteBuffer>> getVertexDbiMap() {
        return vertexDbiMap;
    }

    public Map<String, Dbi<ByteBuffer>> getEdgeDbiMap() {
        return edgeDbiMap;
    }

    public void flush() {
        synchronized (writeLock) {
            if (writeTxn != null) {
                writeTxn.commit();
                writeTxn = null;
            }
        }
        // Force sync if needed
        env.sync(true);

        // Periodically check map size utilization
        flushCounter++;
        if (flushCounter % MAP_SIZE_CHECK_INTERVAL == 0) {
            checkMapSizeUtilization();
        }
    }

    public void compact() {
        // LMDB doesn't need compaction (B+tree structure)
        // This is a no-op for compatibility with the storage interface
        LOGGER.debug("LMDB compact called - no operation needed (B+tree structure)");
    }

    public void checkpoint(String path) {
        LOGGER.info("Creating LMDB checkpoint: {}", path);
        FileUtils.deleteQuietly(new File(path));
        try {
            // Flush any pending writes
            flush();

            // LMDB checkpoint is simply copying the database files
            File sourceDir = new File(filePath);
            File targetDir = new File(path);
            FileUtils.copyDirectory(sourceDir, targetDir);
        } catch (IOException e) {
            throw new GeaflowRuntimeException(RuntimeErrors.INST.runError("lmdb checkpoint error"), e);
        }
    }

    private Txn<ByteBuffer> getWriteTransaction() {
        synchronized (writeLock) {
            if (writeTxn == null) {
                writeTxn = env.txnWrite();
            }
            return writeTxn;
        }
    }

    public void write(String dbName, byte[] key, byte[] value) {
        Dbi<ByteBuffer> dbi = dbiMap.get(dbName);
        if (dbi == null) {
            throw new GeaflowRuntimeException(
                RuntimeErrors.INST.runError("Database not found: " + dbName));
        }

        Txn<ByteBuffer> txn = getWriteTransaction();
        ByteBuffer keyBuffer = ByteBuffer.allocateDirect(key.length);
        keyBuffer.put(key).flip();
        ByteBuffer valueBuffer = ByteBuffer.allocateDirect(value.length);
        valueBuffer.put(value).flip();
        dbi.put(txn, keyBuffer, valueBuffer);
    }

    public void write(Dbi<ByteBuffer> dbi, byte[] key, byte[] value) {
        Txn<ByteBuffer> txn = getWriteTransaction();
        ByteBuffer keyBuffer = ByteBuffer.allocateDirect(key.length);
        keyBuffer.put(key).flip();
        ByteBuffer valueBuffer = ByteBuffer.allocateDirect(value.length);
        valueBuffer.put(value).flip();
        dbi.put(txn, keyBuffer, valueBuffer);
    }

    public void write(String dbName, List<Tuple<byte[], byte[]>> list) {
        Dbi<ByteBuffer> dbi = dbiMap.get(dbName);
        if (dbi == null) {
            throw new GeaflowRuntimeException(
                RuntimeErrors.INST.runError("Database not found: " + dbName));
        }

        Txn<ByteBuffer> txn = getWriteTransaction();
        for (Tuple<byte[], byte[]> tuple : list) {
            ByteBuffer keyBuffer = ByteBuffer.allocateDirect(tuple.f0.length);
            keyBuffer.put(tuple.f0).flip();
            ByteBuffer valueBuffer = ByteBuffer.allocateDirect(tuple.f1.length);
            valueBuffer.put(tuple.f1).flip();
            dbi.put(txn, keyBuffer, valueBuffer);
        }
    }

    public byte[] get(String dbName, byte[] key) {
        Dbi<ByteBuffer> dbi = dbiMap.get(dbName);
        if (dbi == null) {
            throw new GeaflowRuntimeException(
                RuntimeErrors.INST.runError("Database not found: " + dbName));
        }

        try (Txn<ByteBuffer> txn = env.txnRead()) {
            ByteBuffer keyBuffer = ByteBuffer.allocateDirect(key.length);
            keyBuffer.put(key).flip();
            ByteBuffer valueBuffer = dbi.get(txn, keyBuffer);
            if (valueBuffer == null) {
                return null;
            }
            byte[] value = new byte[valueBuffer.remaining()];
            valueBuffer.get(value);
            return value;
        }
    }

    public byte[] get(Dbi<ByteBuffer> dbi, byte[] key) {
        try (Txn<ByteBuffer> txn = env.txnRead()) {
            ByteBuffer keyBuffer = ByteBuffer.allocateDirect(key.length);
            keyBuffer.put(key).flip();
            ByteBuffer valueBuffer = dbi.get(txn, keyBuffer);
            if (valueBuffer == null) {
                return null;
            }
            byte[] value = new byte[valueBuffer.remaining()];
            valueBuffer.get(value);
            return value;
        }
    }

    public void delete(String dbName, byte[] key) {
        Dbi<ByteBuffer> dbi = dbiMap.get(dbName);
        if (dbi == null) {
            throw new GeaflowRuntimeException(
                RuntimeErrors.INST.runError("Database not found: " + dbName));
        }

        Txn<ByteBuffer> txn = getWriteTransaction();
        ByteBuffer keyBuffer = ByteBuffer.allocateDirect(key.length);
        keyBuffer.put(key).flip();
        dbi.delete(txn, keyBuffer);
    }

    public LmdbIterator getIterator(String dbName) {
        Dbi<ByteBuffer> dbi = dbiMap.get(dbName);
        if (dbi == null) {
            throw new GeaflowRuntimeException(
                RuntimeErrors.INST.runError("Database not found: " + dbName));
        }
        return new LmdbIterator(dbi, env.txnRead(), null);
    }

    public LmdbIterator getIterator(Dbi<ByteBuffer> dbi) {
        return new LmdbIterator(dbi, env.txnRead(), null);
    }

    public LmdbIterator getIterator(String dbName, byte[] prefix) {
        Dbi<ByteBuffer> dbi = dbiMap.get(dbName);
        if (dbi == null) {
            throw new GeaflowRuntimeException(
                RuntimeErrors.INST.runError("Database not found: " + dbName));
        }
        return new LmdbIterator(dbi, env.txnRead(), prefix);
    }

    public void close() {
        synchronized (writeLock) {
            if (writeTxn != null) {
                writeTxn.commit();
                writeTxn = null;
            }
        }

        if (env != null) {
            // Close all databases
            dbiMap.values().forEach(Dbi::close);
            vertexDbiMap.values().forEach(Dbi::close);
            edgeDbiMap.values().forEach(Dbi::close);

            // Close environment
            env.close();
            env = null;
        }
    }

    public Dbi<ByteBuffer> getOrCreateDatabase(String dbName) {
        if (!enableDynamicCreateDatabase) {
            throw new GeaflowRuntimeException(
                RuntimeErrors.INST.runError("Dynamic database creation not enabled"));
        }

        Dbi<ByteBuffer> dbi = dbiMap.get(dbName);
        if (dbi == null) {
            dbi = vertexDbiMap.get(dbName);
        }
        if (dbi == null) {
            dbi = edgeDbiMap.get(dbName);
        }

        if (dbi == null) {
            synchronized (this) {
                // Double-check
                dbi = dbiMap.get(dbName);
                if (dbi == null) {
                    dbi = env.openDbi(dbName, DbiFlags.MDB_CREATE);

                    if (dbName.contains(LmdbConfigKeys.VERTEX_DB_PREFIX)) {
                        vertexDbiMap.put(dbName, dbi);
                    } else if (dbName.contains(LmdbConfigKeys.EDGE_DB_PREFIX)) {
                        edgeDbiMap.put(dbName, dbi);
                    } else {
                        dbiMap.put(dbName, dbi);
                    }
                }
            }
        }

        return dbi;
    }

    public void drop() {
        close();
        FileUtils.deleteQuietly(new File(this.filePath));
    }

    public Env<ByteBuffer> getEnv() {
        return env;
    }

    public void checkMapSizeUtilization() {
        try {
            File dbDir = new File(filePath);
            long currentSize = FileUtils.sizeOfDirectory(dbDir);
            long mapSize = config.getLong(LmdbConfigKeys.LMDB_MAP_SIZE);
            double threshold = config.getDouble(LmdbConfigKeys.LMDB_MAP_SIZE_WARNING_THRESHOLD);
            double utilization = (double) currentSize / mapSize;

            if (utilization > threshold) {
                LOGGER.warn("LMDB map size utilization: {:.2f}%, current size: {} bytes, map size: {} bytes. "
                           + "Consider increasing map size.", utilization * 100, currentSize, mapSize);
            } else {
                LOGGER.debug("LMDB map size utilization: {:.2f}%, current size: {} bytes, map size: {} bytes",
                           utilization * 100, currentSize, mapSize);
            }
        } catch (Exception e) {
            LOGGER.warn("Failed to check LMDB map size utilization", e);
        }
    }

    /**
     * Get database statistics.
     * @return Map of database names to their statistics
     */
    public Map<String, DatabaseStats> getDatabaseStats() {
        Map<String, DatabaseStats> stats = new HashMap<>();

        for (Map.Entry<String, Dbi<ByteBuffer>> entry : dbiMap.entrySet()) {
            try (Txn<ByteBuffer> txn = env.txnRead()) {
                org.lmdbjava.Stat stat = entry.getValue().stat(txn);
                stats.put(entry.getKey(), new DatabaseStats(
                    stat.entries,
                    stat.depth,
                    stat.branchPages + stat.leafPages + stat.overflowPages,
                    (stat.branchPages + stat.leafPages + stat.overflowPages) * stat.pageSize
                ));
            } catch (Exception e) {
                LOGGER.warn("Failed to get stats for database: {}", entry.getKey(), e);
            }
        }

        return stats;
    }

    /**
     * Database statistics holder.
     */
    public static class DatabaseStats {
        public final long entries;
        public final int depth;
        public final long pages;
        public final long sizeBytes;

        public DatabaseStats(long entries, int depth, long pages, long sizeBytes) {
            this.entries = entries;
            this.depth = depth;
            this.pages = pages;
            this.sizeBytes = sizeBytes;
        }

        @Override
        public String toString() {
            return String.format("DatabaseStats{entries=%d, depth=%d, pages=%d, sizeBytes=%d}",
                               entries, depth, pages, sizeBytes);
        }
    }
}
