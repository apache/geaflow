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

package org.apache.geaflow.infer.exchange;

import java.io.Closeable;
import java.io.RandomAccessFile;
import java.lang.reflect.Method;
import java.nio.channels.FileChannel;
import org.apache.geaflow.common.exception.GeaflowRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.nio.ch.FileChannelImpl;

public class MemoryMapper implements Closeable {

    private static final Logger LOGGER = LoggerFactory.getLogger(MemoryMapper.class);
    private static final String MAP_0 = "map0";
    private static final String UNMAP_0 = "unmap0";
    private static final Method MEMORY_MAP_METHOD;
    private static final String SHARE_MEMORY_MODE = "rw";
    private static final Method MEMORY_UN_MAP_METHOD;
    private long mapAddress;
    private final long mapSize;
    private final String mapKey;

    static {
        try {
            MEMORY_MAP_METHOD = getMethod(FileChannelImpl.class, MAP_0, int.class, long.class, long.class);
            MEMORY_UN_MAP_METHOD = getMethod(FileChannelImpl.class, UNMAP_0, long.class, long.class);
        } catch (Exception e) {
            throw new GeaflowRuntimeException(e);
        }
    }

    public MemoryMapper(String mapKey, long size) {
        this.mapKey = mapKey;
        this.mapSize = roundTo4096(size);
        map(mapKey, mapSize);
    }


    private void map(String mapKey, long mapSize) {
        try {
            RandomAccessFile backingFile = new RandomAccessFile(mapKey, SHARE_MEMORY_MODE);
            backingFile.setLength(mapSize);
            FileChannel ch = backingFile.getChannel();
            this.mapAddress = (long) MEMORY_MAP_METHOD.invoke(ch, 1, 0L, mapSize);
            ch.close();
            backingFile.close();
        } catch (Throwable e) {
            LOGGER.error("map memory key {} size {} failed", mapKey, mapSize);
            throw new GeaflowRuntimeException("memory map failed", e);
        }
    }


    public void unMap() {
        if (mapAddress != 0) {
            try {
                MEMORY_UN_MAP_METHOD.invoke(null, mapAddress, this.mapSize);
            } catch (Throwable e) {
                LOGGER.error("un map error");
                mapAddress = 0;
            }
        }
        mapAddress = 0;
    }

    @Override
    public void close() {
        unMap();
    }

    public long getMapSize() {
        return mapSize;
    }

    public long getMapAddress() {
        return mapAddress;
    }


    private static Method getMethod(Class<?> cls, String name, Class<?>... params)
        throws Exception {
        Method m = cls.getDeclaredMethod(name, params);
        m.setAccessible(true);
        return m;
    }

    private long roundTo4096(long i) {
        return (i + 0xfffL) & ~0xfffL;
    }
}
