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

package org.apache.geaflow.memory;

import io.netty.util.concurrent.FastThreadLocalThread;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.memory.config.MemoryConfigKeys;
import org.apache.geaflow.memory.exception.GeaflowOutOfMemoryException;
import org.testng.Assert;
import org.testng.annotations.Test;

public class ThreadMemoryTest extends MemoryReleaseTest {

    private static final int PAGE_SIZE = 8192;
    private static final int PAGE_SHIFTS = 11;

    @Test
    public void testStream() throws Throwable {
        Map<String, String> conf = new HashMap<>();
        conf.put(MemoryConfigKeys.ON_HEAP_MEMORY_SIZE_MB.getKey(), "1024");
        conf.put(MemoryConfigKeys.MEMORY_MAX_ORDER.getKey(), "" + PAGE_SHIFTS);
        conf.put(MemoryConfigKeys.MEMORY_PAGE_SIZE.getKey(), "" + PAGE_SIZE);

        MemoryManager memoryManager = MemoryManager.build(new Configuration(conf));
        LinkedBlockingQueue<MemoryView> views = new LinkedBlockingQueue<>(10);

        int threadNum = 8;
        ExecutorService executor = Executors.newFixedThreadPool(threadNum);
        executor.execute(new FastThreadLocalThread(() -> {
            while (true) {
                MemoryView v = views.poll();
                if (v != null) {
                    v.close();
                }
            }
        }));

        final CountDownLatch latch = new CountDownLatch(threadNum);
        long start = System.currentTimeMillis();
        for (int i = 0; i < threadNum; i++) {
            executor.execute(new FastThreadLocalThread(() -> {
                for (int j = 0; j < 10000; j++) {
                    try {
                        MemoryView view = memoryManager.requireMemory(ESegmentSize.S65536.size(),
                            MemoryGroupManger.SHUFFLE);
                        while (!views.offer(view)) {
                            ;
                        }
                    } catch (Exception ex) {
                        System.out.println(ex);
                    }
                }
                latch.countDown();
            }));
        }
        latch.await();
        System.out.println(System.currentTimeMillis() - start);

        Thread.sleep(1000);
        System.out.println(memoryManager);
    }

    @Test
    public void testBatch() throws Throwable {
        Map<String, String> config = new HashMap<>();
        config.put(MemoryConfigKeys.ON_HEAP_MEMORY_SIZE_MB.getKey(), "1024");
        config.put(MemoryConfigKeys.MEMORY_MAX_ORDER.getKey(), "" + PAGE_SHIFTS);
        config.put(MemoryConfigKeys.MEMORY_PAGE_SIZE.getKey(), "" + PAGE_SIZE);

        MemoryManager memoryManager = MemoryManager.build(new Configuration(config));
        LinkedBlockingQueue<MemoryView> views = new LinkedBlockingQueue<>();

        int threadNum = 8;
        ExecutorService executor = Executors.newFixedThreadPool(threadNum);
        final CountDownLatch latch = new CountDownLatch(threadNum);
        long start = System.currentTimeMillis();
        for (int i = 0; i < threadNum; i++) {
            executor.execute(() -> {
                for (int j = 0; j < 100; j++) {
                    try {
                        MemoryView view = memoryManager.requireMemory(32,
                            MemoryGroupManger.SHUFFLE);
                        while (!views.offer(view)) {
                            ;
                        }
                    } catch (Exception ex) {
                        System.out.println(ex);
                    }
                }
                latch.countDown();
                memoryManager.localCache(MemoryGroupManger.SHUFFLE).free();
            });
        }
        latch.await();
        System.out.println(System.currentTimeMillis() - start);
        Thread.sleep(1000);

        System.out.println(memoryManager);
        while (true) {
            MemoryView v = views.poll();
            if (v != null) {
                v.close();
            } else {
                break;
            }
        }

        System.out.println(memoryManager);

        final CountDownLatch latch2 = new CountDownLatch(threadNum);
        start = System.currentTimeMillis();
        for (int i = 0; i < threadNum; i++) {
            executor.execute(() -> {
                for (int j = 0; j < 100; j++) {
                    try {
                        MemoryView view = memoryManager.requireMemory(32768,
                            MemoryGroupManger.SHUFFLE);
                        while (!views.offer(view)) {
                            ;
                        }
                    } catch (Exception ex) {
                        System.out.println(ex);
                    }
                }
                latch2.countDown();
            });
        }
        latch2.await();
        System.out.println(System.currentTimeMillis() - start);
        Thread.sleep(1000);

        while (true) {
            MemoryView v = views.poll();
            if (v != null) {
                v.close();
            } else {
                break;
            }
        }

        final CountDownLatch latch3 = new CountDownLatch(threadNum);
        start = System.currentTimeMillis();
        for (int i = 0; i < threadNum; i++) {
            executor.execute(() -> {
                for (int j = 0; j < 100; j++) {
                    try {
                        MemoryView view = memoryManager.requireMemory(32768,
                            MemoryGroupManger.SHUFFLE);
                        while (!views.offer(view)) {
                            ;
                        }
                    } catch (Exception ex) {
                        System.out.println(ex);
                    }
                }
                latch3.countDown();
            });
        }
        latch3.await();
        System.out.println(System.currentTimeMillis() - start);
        Thread.sleep(1000);

        while (true) {
            MemoryView v = views.poll();
            if (v != null) {
                v.close();
            } else {
                break;
            }
        }

        System.out.println(memoryManager);
        Thread.sleep(1000);
        System.out.println(memoryManager);
    }

    @Test
    public void testOOM() {
        Assert.assertThrows(GeaflowOutOfMemoryException.class, () -> {
            Map<String, String> config = new HashMap<>();
            config.put(MemoryConfigKeys.OFF_HEAP_MEMORY_SIZE_MB.getKey(), "1");
            config.put(MemoryConfigKeys.MAX_DIRECT_MEMORY_SIZE.getKey(), "" + 1024 * 1024);
            config.put(MemoryConfigKeys.MEMORY_AUTO_ADAPT_ENABLE.getKey(), "false");

            MemoryManager memoryManager = MemoryManager.build(new Configuration(config));
            MemoryView view = memoryManager.requireMemory(1024 * 1024 * 10, MemoryGroupManger.DEFAULT);
        });
    }
}
