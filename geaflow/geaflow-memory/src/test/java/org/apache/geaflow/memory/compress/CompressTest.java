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

package org.apache.geaflow.memory.compress;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.common.serialize.SerializerFactory;
import org.apache.geaflow.memory.MemoryGroupManger;
import org.apache.geaflow.memory.MemoryManager;
import org.apache.geaflow.memory.MemoryView;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class CompressTest {

    @BeforeClass
    public static void setUp() {
        MemoryManager.build(new Configuration());
    }

    @AfterClass
    public static void after() {
        MemoryManager.getInstance().dispose();
    }

    @Test
    public void testSnappy() throws IOException {

        Map<String, String> map = new HashMap<>();
        for (int i = 10; i < 100; i++) {
            map.put(Integer.toString(i), Integer.toString(i));
        }
        byte[] bMap = SerializerFactory.getKryoSerializer().serialize(map);
        MemoryView cc = MemoryManager.getInstance().requireMemory(bMap.length, MemoryGroupManger.DEFAULT);
        cc.getWriter().write(bMap);
        cc = Snappy.uncompress(Snappy.compress(cc));
        Assert.assertEquals(cc.toArray(), bMap);
    }

    @Test
    public void testLz4() throws IOException {
        Map<String, String> map = new HashMap<>();
        for (int i = 10; i < 100; i++) {
            map.put(Integer.toString(i), Integer.toString(i));
        }
        byte[] bMap = SerializerFactory.getKryoSerializer().serialize(map);
        MemoryView cc = MemoryManager.getInstance().requireMemory(bMap.length, MemoryGroupManger.DEFAULT);
        cc.getWriter().write(bMap);
        cc = Lz4.uncompress(Lz4.compress(cc));
        Assert.assertEquals(cc.toArray(), bMap);
    }

    @Test
    public void multiThreadSnappy() {

        Map<String, String> map = new HashMap<>();
        for (int i = 0; i < 10000; i++) {
            map.put(Integer.toString(i), Integer.toString(i));
        }
        byte[] bMap = SerializerFactory.getKryoSerializer().serialize(map);

        int threadNum = 5;
        ExecutorService executor = Executors.newFixedThreadPool(threadNum);
        List<Future> list = new ArrayList<>();
        for (int i = 0; i < threadNum; i++) {
            list.add(executor.submit(() -> {
                try {
                    MemoryView view = MemoryManager.getInstance().requireMemory(bMap.length,
                        MemoryGroupManger.DEFAULT);
                    view.getWriter().write(bMap);
                    byte[] after = Snappy.uncompress(Snappy.compress(view)).toArray();
                    Assert.assertEquals(after, bMap);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }));
        }
        list.forEach(c -> {
            try {
                c.get();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

    @Test
    public void multiThreadLz4() {
        Map<String, String> map = new HashMap<>();
        for (int i = 0; i < 10000; i++) {
            map.put(Integer.toString(i), Integer.toString(i));
        }
        byte[] bMap = SerializerFactory.getKryoSerializer().serialize(map);

        int threadNum = 5;
        ExecutorService executor = Executors.newFixedThreadPool(threadNum);
        List<Future> list = new ArrayList<>();
        for (int i = 0; i < threadNum; i++) {
            list.add(executor.submit(() -> {
                try {
                    MemoryView view = MemoryManager.getInstance().requireMemory(bMap.length,
                        MemoryGroupManger.DEFAULT);
                    view.getWriter().write(bMap);
                    byte[] after = Lz4.uncompress(Lz4.compress(view)).toArray();
                    Assert.assertEquals(after, bMap);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }));
        }
        list.forEach(c -> {
            try {
                c.get();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }
}
