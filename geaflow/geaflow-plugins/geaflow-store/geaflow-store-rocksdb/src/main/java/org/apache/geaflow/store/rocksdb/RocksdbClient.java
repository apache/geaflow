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

package org.apache.geaflow.store.rocksdb;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.commons.io.FileUtils;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.common.config.keys.StateConfigKeys;
import org.apache.geaflow.common.errorcode.RuntimeErrors;
import org.apache.geaflow.common.exception.GeaflowRuntimeException;
import org.apache.geaflow.common.tuple.Tuple;
import org.apache.geaflow.store.rocksdb.options.IRocksDBOptions;
import org.rocksdb.Checkpoint;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.TtlDB;
import org.rocksdb.WriteBatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RocksdbClient {

    private static final Logger LOGGER = LoggerFactory.getLogger(RocksdbClient.class);

    private final String filePath;
    private final String optionClass;
    private final Configuration config;
    private final List<String> cfList;
    private RocksDB rocksdb;
    private IRocksDBOptions rocksDBOptions;
    // column family name -> vertex or edge column family handle
    private final Map<String, ColumnFamilyHandle> handleMap = new HashMap<>();
    private final Map<String, ColumnFamilyHandle> vertexHandleMap = new ConcurrentHashMap<>();
    private final Map<String, ColumnFamilyHandle> edgeHandleMap = new ConcurrentHashMap<>();

    // column family name -> column family descriptor
    private Map<String, ColumnFamilyDescriptor> descriptorMap;
    private boolean enableDynamicCreateColumnFamily;

    public RocksdbClient(String filePath, List<String> cfList, Configuration config,
                         boolean enableDynamicCreateColumnFamily) {
        this(filePath, cfList, config);
        this.enableDynamicCreateColumnFamily = enableDynamicCreateColumnFamily;

        if (enableDynamicCreateColumnFamily) {
            // Using concurrent hashmap in partition situation
            descriptorMap = new ConcurrentHashMap<>();
        } else {
            descriptorMap = new HashMap<>();
        }
    }

    public RocksdbClient(String filePath, List<String> cfList, Configuration config) {
        this.filePath = filePath;
        this.cfList = cfList;
        this.config = config;
        this.optionClass = this.config.getString(RocksdbConfigKeys.ROCKSDB_OPTION_CLASS);
    }

    private void initRocksDbOptions() {
        if (this.rocksDBOptions == null || this.rocksDBOptions.isClosed()) {
            LOGGER.info("rocksdb optionClass {}", optionClass);
            try {
                this.rocksDBOptions = (IRocksDBOptions) Class.forName(optionClass).newInstance();
                this.rocksDBOptions.init(config);
            } catch (Throwable e) {
                LOGGER.error("{} not found", optionClass);
                throw new GeaflowRuntimeException(
                    RuntimeErrors.INST.runError(optionClass + "class not found"), e);
            }

            if (this.config.getBoolean(RocksdbConfigKeys.ROCKSDB_STATISTICS_ENABLE)) {
                this.rocksDBOptions.enableStatistics();
            }
            if (this.config.getBoolean(StateConfigKeys.STATE_PARANOID_CHECK_ENABLE)) {
                this.rocksDBOptions.enableParanoidCheck();
            }
        }
    }

    public void initDB() {
        File dbFile = new File(filePath);
        if (!dbFile.getParentFile().exists()) {
            try {
                FileUtils.forceMkdir(dbFile.getParentFile());
            } catch (IOException e) {
                throw new GeaflowRuntimeException(RuntimeErrors.INST.runError("create file error"),
                    e);
            }
        }

        if (rocksdb == null) {
            initRocksDbOptions();
            LOGGER.info("ThreadId {}, buildDB {}", Thread.currentThread().getId(), filePath);
            int ttl = this.config.getInteger(RocksdbConfigKeys.ROCKSDB_TTL_SECOND);
            this.descriptorMap.clear();
            List<ColumnFamilyHandle> handles = new ArrayList<>();
            List<Integer> ttls = new ArrayList<>();

            List<String> validCfList = cfList;
            if (enableDynamicCreateColumnFamily) {
                try {
                    List<byte[]> cfNames = RocksDB.listColumnFamilies(new Options(), filePath);
                    if (!cfNames.isEmpty()) {
                        validCfList = new ArrayList<>();
                        for (byte[] cfName : cfNames) {
                            validCfList.add(new String(cfName));
                        }
                    }
                } catch (RocksDBException e) {
                    throw new GeaflowRuntimeException(
                        RuntimeErrors.INST.runError("List column family error"), e);
                }
            }

            List<ColumnFamilyDescriptor> descriptorList = new ArrayList<>();
            for (String name : validCfList) {
                ColumnFamilyDescriptor descriptor = new ColumnFamilyDescriptor(name.getBytes(),
                    rocksDBOptions.buildFamilyOptions());
                descriptorList.add(descriptor);
                descriptorMap.put(name, descriptor);
                ttls.add(ttl);
            }

            try {
                rocksdb = TtlDB.open(rocksDBOptions.getDbOptions(), this.filePath, descriptorList,
                    handles, ttls, false);
            } catch (Exception e) {
                throw new GeaflowRuntimeException(RuntimeErrors.INST.runError("open rocksdb error"),
                    e);
            }

            if (enableDynamicCreateColumnFamily) {
                for (int i = 0; i < validCfList.size(); i++) {
                    if (validCfList.get(i).contains(RocksdbConfigKeys.VERTEX_CF_PREFIX)) {
                        vertexHandleMap.put(validCfList.get(i), handles.get(i));
                    } else if (validCfList.get(i).contains(RocksdbConfigKeys.EDGE_CF_PREFIX)) {
                        edgeHandleMap.put(validCfList.get(i), handles.get(i));
                    } else {
                        handleMap.put(validCfList.get(i), handles.get(i));
                    }
                }
            } else {
                for (int i = 0; i < validCfList.size(); i++) {
                    handleMap.put(validCfList.get(i), handles.get(i));
                }
            }
        }
    }

    public Map<String, ColumnFamilyHandle> getColumnFamilyHandleMap() {
        return handleMap;
    }

    public Map<String, ColumnFamilyHandle> getVertexHandleMap() {
        return vertexHandleMap;
    }

    public Map<String, ColumnFamilyHandle> getEdgeHandleMap() {
        return edgeHandleMap;
    }

    public void flush() {
        try {
            this.rocksdb.flush(rocksDBOptions.getFlushOptions());
        } catch (RocksDBException e) {
            throw new GeaflowRuntimeException(RuntimeErrors.INST.runError("rocksdb compact error"),
                e);
        }
    }


    public void compact() {
        try {
            this.rocksdb.compactRange();
        } catch (RocksDBException e) {
            throw new GeaflowRuntimeException(RuntimeErrors.INST.runError("rocksdb compact error"),
                e);
        }
    }

    public void checkpoint(String path) {
        Checkpoint checkpoint = Checkpoint.create(rocksdb);
        LOGGER.info("Delete path: {}", path);
        FileUtils.deleteQuietly(new File(path));
        try {
            checkpoint.createCheckpoint(path);
        } catch (RocksDBException e) {
            throw new GeaflowRuntimeException(RuntimeErrors.INST.runError("rocksdb chk error"), e);
        }
        checkpoint.close();
    }

    public void write(String cf, byte[] key, byte[] value) {
        try {
            this.rocksdb.put(handleMap.get(cf), key, value);
        } catch (RocksDBException e) {
            throw new GeaflowRuntimeException(RuntimeErrors.INST.runError("rocksdb put error"), e);
        }
    }

    public void write(ColumnFamilyHandle handle, byte[] key, byte[] value) {
        try {
            this.rocksdb.put(handle, key, value);
        } catch (RocksDBException e) {
            throw new GeaflowRuntimeException(RuntimeErrors.INST.runError("rocksdb put error"), e);
        }
    }

    public void write(WriteBatch writeBatch) {
        try {
            this.rocksdb.write(rocksDBOptions.getWriteOptions(), writeBatch);
        } catch (RocksDBException e) {
            throw new GeaflowRuntimeException(RuntimeErrors.INST.runError("rocksdb put error"), e);
        }
    }

    public void write(String cf, List<Tuple<byte[], byte[]>> list) {
        try {
            WriteBatch writeBatch = new WriteBatch();
            for (Tuple<byte[], byte[]> tuple : list) {
                writeBatch.put(handleMap.get(cf), tuple.f0, tuple.f1);
            }
            this.rocksdb.write(rocksDBOptions.getWriteOptions(), writeBatch);
            writeBatch.clear();
            writeBatch.close();
        } catch (RocksDBException e) {
            throw new GeaflowRuntimeException(RuntimeErrors.INST.runError("rocksdb put error"), e);
        }
    }

    public byte[] get(String cf, byte[] key) {
        try {
            return this.rocksdb.get(handleMap.get(cf), key);
        } catch (RocksDBException e) {
            throw new GeaflowRuntimeException(RuntimeErrors.INST.runError("rocksdb get error"), e);
        }
    }

    public byte[] get(ColumnFamilyHandle handle, byte[] key) {
        try {
            return this.rocksdb.get(handle, key);
        } catch (RocksDBException e) {
            throw new GeaflowRuntimeException(RuntimeErrors.INST.runError("rocksdb get error"), e);
        }
    }

    public void delete(String cf, byte[] key) {
        try {
            this.rocksdb.delete(handleMap.get(cf), key);
        } catch (RocksDBException e) {
            throw new GeaflowRuntimeException(RuntimeErrors.INST.runError("rocksdb delete error"),
                e);
        }
    }

    public RocksIterator getIterator(String cf) {
        return this.rocksdb.newIterator(handleMap.get(cf));
    }

    public RocksIterator getIterator(ColumnFamilyHandle handle) {
        return this.rocksdb.newIterator(handle);
    }

    public void close() {
        if (rocksdb != null) {
            this.rocksdb.close();
            this.rocksDBOptions.close();
            this.descriptorMap.forEach((k, d) -> d.getOptions().close());
            this.rocksDBOptions = null;
            this.rocksdb = null;
        }
    }

    public void drop() {
        close();
        FileUtils.deleteQuietly(new File(this.filePath));
    }

    public RocksDB getRocksdb() {
        return rocksdb;
    }

    public IRocksDBOptions getRocksDBOptions() {
        return rocksDBOptions;
    }

    public Map<String, ColumnFamilyDescriptor> getDescriptorMap() {
        return descriptorMap;
    }
}
