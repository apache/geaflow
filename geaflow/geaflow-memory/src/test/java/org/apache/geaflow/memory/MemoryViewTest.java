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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.common.utils.MemoryUtils;
import org.apache.geaflow.memory.config.MemoryConfigKeys;
import org.testng.Assert;
import org.testng.annotations.Test;

public class MemoryViewTest extends MemoryReleaseTest {

    private static final int PAGE_SIZE = 8192;
    private static final int PAGE_SHIFTS = 11;

    @Test
    public void testView() {

        Map<String, String> conf = Maps.newHashMap();
        conf.put(MemoryConfigKeys.OFF_HEAP_MEMORY_SIZE_MB.getKey(), "32");
        conf.put(MemoryConfigKeys.MEMORY_MAX_ORDER.getKey(), "" + PAGE_SHIFTS);
        conf.put(MemoryConfigKeys.MEMORY_PAGE_SIZE.getKey(), "" + PAGE_SIZE);
        MemoryManager.build(new Configuration(conf));
        System.out.println(MemoryManager.getInstance().toString());

        MemoryView view = MemoryManager.getInstance()
            .requireMemory(1200, MemoryGroupManger.SHUFFLE);
        byte[] a = new byte[999];
        Arrays.fill(a, (byte) 1);
        byte[] c = new byte[100];
        ByteBuffer bf = ByteBuffer.wrap(new byte[1100]);
        bf.put(a);
        bf.put(c);
        bf.put((byte) 3);

        MemoryViewWriter writer = view.getWriter();
        writer.write(a);
        writer.write(c);
        writer.write(3);
        Assert.assertEquals(view.remain(), 948);
        byte[] b = view.toArray();
        Assert.assertEquals(b, bf.array());
        view.close();

        view = MemoryManager.getInstance().requireMemory(1600, MemoryGroupManger.SHUFFLE);
        writer = view.getWriter();
        writer.write(a);
        writer.write(c);
        writer.write(3);
        Assert.assertEquals(view.remain(), 1024 * 2 - 1100);
        b = view.toArray();
        Assert.assertEquals(b, bf.array());
        view.close();

        MemoryManager.getInstance().dispose();

        conf.remove(MemoryConfigKeys.OFF_HEAP_MEMORY_SIZE_MB.getKey());
        conf.put(MemoryConfigKeys.ON_HEAP_MEMORY_SIZE_MB.getKey(), "32");
        MemoryManager.build(new Configuration(conf));

        view = MemoryManager.getInstance().requireMemory(1200, MemoryGroupManger.SHUFFLE);
        writer = view.getWriter();
        writer.write(a);
        writer.write(c);
        writer.write(3);
        Assert.assertEquals(view.remain(), 1024 * 2 - 1100);
        b = view.toArray();
        Assert.assertEquals(b, bf.array());
        view.close();

        view = MemoryManager.getInstance().requireMemory(1600, MemoryGroupManger.SHUFFLE);
        writer = view.getWriter();
        writer.write(a);
        writer.write(c);
        writer.write(3);
        Assert.assertEquals(view.remain(), 1024 * 2 - 1100);
        b = view.toArray();
        Assert.assertEquals(b, bf.array());

        MemoryView view2 = MemoryManager.getInstance()
            .requireMemory(bf.array().length, MemoryGroupManger.SHUFFLE);
        writer = view2.getWriter();
        writer.write(bf.array());

        Assert.assertEquals(view.hashCode(), view2.hashCode());
        Assert.assertEquals(view, view2);
        view.close();
        view2.close();

        System.out.println(MemoryManager.getInstance().totalAllocateHeapMemory());
        System.out.println(MemoryManager.getInstance().totalAllocateOffHeapMemory());
        System.out.println(MemoryManager.getInstance().usedHeapMemory());
        System.out.println(MemoryManager.getInstance().usedOffHeapMemory());
    }

    @Test(priority = 1)
    public void release() throws Throwable {
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            Map<String, String> conf = Maps.newHashMap();
            conf.put(MemoryConfigKeys.OFF_HEAP_MEMORY_SIZE_MB.getKey(), "32");
            conf.put(MemoryConfigKeys.MEMORY_MAX_ORDER.getKey(), "" + PAGE_SHIFTS);
            conf.put(MemoryConfigKeys.MEMORY_PAGE_SIZE.getKey(), "" + PAGE_SIZE);
            MemoryManager.build(new Configuration(conf));

            byte[] a = new byte[999];
            for (int i = 0; i < a.length; i++) {
                a[i] = 1;
            }
            byte[] c = new byte[100];
            for (int i = 0; i < c.length; i++) {
                c[i] = 0;
            }
            ByteBuffer bf = ByteBuffer.wrap(new byte[10099]);
            bf.put(c);
            bf.put(a);

            MemoryView view = MemoryManager.getInstance()
                .requireMemory(bf.array().length, MemoryGroupManger.SHUFFLE);
            view.getWriter().write(bf.array());
            view.close();

            view.remain();
        });
    }

    @Test(priority = 2)
    public void testReadWrite() throws Throwable {
        Map<String, String> conf = Maps.newHashMap();
        conf.put(MemoryConfigKeys.ON_HEAP_MEMORY_SIZE_MB.getKey(), "32");
        conf.put(MemoryConfigKeys.MEMORY_MAX_ORDER.getKey(), "" + PAGE_SHIFTS);
        conf.put(MemoryConfigKeys.MEMORY_PAGE_SIZE.getKey(), "" + PAGE_SIZE);
        MemoryManager.build(new Configuration(conf));

        MemoryView heapView = MemoryManager.getInstance()
            .requireMemory(64, MemoryGroupManger.SHUFFLE);
        String a = "MemoryView heapView = memoryManager.requireMemory(1025, MemoryMode.ON_HEAP";

        byte[] content = a.getBytes();
        MemoryViewWriter writer = heapView.getWriter(64);
        writer.write(content);
        Assert.assertEquals(new String(heapView.toArray()), a);

        writer.write(content, 0, 50);
        writer.write(content, 50, content.length - 50);

        Assert.assertEquals(new String(heapView.toArray()), a + a);

        MemoryViewReader reader = heapView.getReader();
        byte[] readContent = new byte[content.length * 2];
        for (int i = 0; i < readContent.length; i++) {
            readContent[i] = reader.read();
        }
        Assert.assertEquals(new String(readContent), a + a);
        reader.reset();

        readContent = new byte[content.length];
        reader.read(readContent, 0, 50);
        reader.read(readContent, 50, content.length - 50);
        Assert.assertEquals(new String(readContent), a);

        heapView = MemoryManager.getInstance().requireMemory(1025, MemoryGroupManger.SHUFFLE);
        writer = heapView.getWriter();
        writer.write(new byte[101]);
        writer.write(content);

        readContent = new byte[content.length];
        reader = heapView.getReader();
        reader.skip(101);
        reader.read(readContent);
        Assert.assertEquals(new String(readContent), a);

        heapView = MemoryManager.getInstance().requireMemory(62, MemoryGroupManger.SHUFFLE);
        writer = heapView.getWriter();
        for (int i = 0; i < 5; i++) {
            writer.write(content);
        }
        Assert.assertEquals(new String(heapView.toArray()), a + a + a + a + a);
    }

    @Test(priority = 3)
    public void testReadWrite2() throws Throwable {
        Map<String, String> conf = Maps.newHashMap();
        conf.put(MemoryConfigKeys.ON_HEAP_MEMORY_SIZE_MB.getKey(), "32");
        conf.put(MemoryConfigKeys.MEMORY_MAX_ORDER.getKey(), "" + PAGE_SHIFTS);
        conf.put(MemoryConfigKeys.MEMORY_PAGE_SIZE.getKey(), "" + PAGE_SIZE);
        MemoryManager.build(new Configuration(conf));

        byte[] a = new byte[64 + 128];
        for (int i = 0; i < a.length; i++) {
            a[i] = (byte) i;
        }
        MemoryView heapView = MemoryManager.getInstance()
            .requireMemory(64, MemoryGroupManger.SHUFFLE);
        MemoryViewWriter writer = heapView.getWriter();
        writer.write(a);

        MemoryViewReader reader = heapView.getReader();
        byte[] b = new byte[a.length];
        reader.read(b);
        Assert.assertEquals(reader.read(b, 64, 0), 0);

        Assert.assertEquals(a, b);

        writer.write(0);
        Assert.assertEquals(reader.read(), 0);

        writer.write(0);
        writer.write(0);
        writer.write(1);
        Assert.assertEquals(heapView.contentSize, 64 + 128 + 4);
        reader.skip(1);
        reader.skip(1);
        Assert.assertEquals(reader.read(), 1);

        Assert.assertEquals(reader.skip(1), 0);

        byte[] t = new byte[4];
        Assert.assertEquals(reader.read(t, 0, t.length), -1);
    }

    @Test
    public void testReadAndWriteStream() throws IOException {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 100000; i++) {
            sb.append("hello world");
        }

        String sentence = sb.toString();
        byte[] bytes = sentence.getBytes();
        ByteArrayInputStream inputStream = new ByteArrayInputStream(bytes);

        Map<String, String> config = new HashMap<>();
        config.put(MemoryConfigKeys.OFF_HEAP_MEMORY_SIZE_MB.getKey(), "32");
        MemoryManager memoryManager = MemoryManager.build(new Configuration(config));

        MemoryView heapView = memoryManager.requireMemory(bytes.length, MemoryGroupManger.SHUFFLE);
        MemoryViewWriter writer = heapView.getWriter();
        writer.write(inputStream, bytes.length);

        ByteArrayOutputStream outputStream = new ByteArrayOutputStream(bytes.length);
        MemoryViewReader reader = heapView.getReader();
        reader.read(outputStream);

        String res = outputStream.toString();
        Assert.assertEquals(res, sentence);

    }


    @Test
    public void testMultiThread() {
        Map<String, String> config = new HashMap<>();
        config.put(MemoryConfigKeys.OFF_HEAP_MEMORY_SIZE_MB.getKey(), "32");
        ExecutorService executors = Executors.newFixedThreadPool(2);
        MemoryManager memoryManager = MemoryManager.build(new Configuration(config));

        executors.submit(() -> {
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < 100000; i++) {
                sb.append("hello world");
            }

            String sentence = sb.toString();
            byte[] bytes = sentence.getBytes();
            ByteArrayInputStream inputStream = new ByteArrayInputStream(bytes);

            MemoryView heapView = memoryManager.requireMemory(bytes.length,
                MemoryGroupManger.SHUFFLE);
            MemoryViewWriter writer = heapView.getWriter();
            try {
                writer.write(inputStream, bytes.length);
            } catch (IOException e) {
                e.printStackTrace();
            }

            ByteArrayOutputStream outputStream = new ByteArrayOutputStream(bytes.length);
            MemoryViewReader reader = heapView.getReader();
            try {
                reader.read(outputStream);
            } catch (IOException e) {
                e.printStackTrace();
            }

            String res = outputStream.toString();
            //outputStream.size()
            Assert.assertEquals(res, sentence);
        });
    }

    @Test
    public void testInitBufs() {
        Map<String, String> config = new HashMap<>();
        config.put(MemoryConfigKeys.OFF_HEAP_MEMORY_SIZE_MB.getKey(), "320");
        MemoryManager manager = MemoryManager.build(new Configuration(config));

        MemoryView view = manager.requireMemory(28948888, MemoryGroupManger.SHUFFLE);

        Assert.assertTrue(view.getBufList().size() == 2);
        Assert.assertTrue(view.getBufList().get(0).getLength() == ESegmentSize.S16777216.size());
        Assert.assertTrue(view.getBufList().get(1).getLength() == ESegmentSize.S16777216.size());

        MemoryView view1 = manager.requireMemory(
            ESegmentSize.S16777216.size() + ESegmentSize.S16.size() + 2, MemoryGroupManger.SHUFFLE);

        Assert.assertTrue(view1.getBufList().size() == 2);
        Assert.assertTrue(view1.getBufList().get(0).getLength() == ESegmentSize.S16777216.size());
        Assert.assertTrue(view1.getBufList().get(1).getLength() == ESegmentSize.S32.size());
    }


    private void testReuse() {

        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 10000; i++) {
            sb.append("hello world");
        }

        byte[] bytes = sb.toString().getBytes();
        MemoryView view = MemoryManager.getInstance()
            .requireMemory(bytes.length, MemoryGroupManger.SHUFFLE);

        view.getWriter().write(bytes);
        view.getWriter().write(bytes);
        view.getWriter().write(bytes);

        byte[] bytes1 = new byte[bytes.length];
        view.getReader().read(bytes1);
        Assert.assertEquals(sb.toString(), new String(bytes1));

        view.reset();

        Random random = new Random(2000);

        sb = new StringBuilder();
        for (int i = 0; i < random.nextInt(2000); i++) {
            sb.append("hello world");
        }

        bytes = sb.toString().getBytes();

        view.getWriter().write(bytes);
        view.getWriter().write(bytes);
        view.getWriter().write(bytes);

        bytes1 = new byte[bytes.length];
        view.getReader().read(bytes1);
        Assert.assertEquals(sb.toString(), new String(bytes1));

        view.reset();

        sb = new StringBuilder();
        for (int i = 0; i < random.nextInt(2000); i++) {
            sb.append("hello world");
        }

        bytes = sb.toString().getBytes();

        view.getWriter().write(bytes);
        view.getWriter().write(bytes);
        view.getWriter().write(bytes);

        bytes1 = new byte[bytes.length + 10];

        MemoryViewReader reader = view.getReader();
        reader.skip(bytes.length * 2);
        reader.read(bytes1);

        byte[] bytes2 = Arrays.copyOf(bytes1, bytes.length);
        Assert.assertEquals(sb.toString(), new String(bytes2));

        byte[] bytes3 = Arrays.copyOfRange(bytes1, bytes.length, bytes.length + 10);

        Assert.assertEquals(bytes3, new byte[10]);

        view.close();
    }

    private void testReNew() {

        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 10000; i++) {
            sb.append("hello world");
        }

        byte[] bytes = sb.toString().getBytes();
        MemoryView view1 = MemoryManager.getInstance()
            .requireMemory(bytes.length, MemoryGroupManger.SHUFFLE);

        view1.getWriter().write(bytes);
        view1.getWriter().write(bytes);
        view1.getWriter().write(bytes);

        byte[] bytes1 = new byte[bytes.length];
        view1.getReader().read(bytes1);
        Assert.assertEquals(sb.toString(), new String(bytes1));

        Random random = new Random(2000);

        sb = new StringBuilder();
        for (int i = 0; i < random.nextInt(2000); i++) {
            sb.append("hello world");
        }

        bytes = sb.toString().getBytes();
        MemoryView view = MemoryManager.getInstance()
            .requireMemory(bytes.length, MemoryGroupManger.SHUFFLE);

        view.getWriter().write(bytes);
        view.getWriter().write(bytes);
        view.getWriter().write(bytes);

        bytes1 = new byte[bytes.length];
        view.getReader().read(bytes1);
        Assert.assertEquals(sb.toString(), new String(bytes1));

        sb = new StringBuilder();
        for (int i = 0; i < random.nextInt(2000); i++) {
            sb.append("hello world");
        }

        bytes = sb.toString().getBytes();
        MemoryView view2 = MemoryManager.getInstance()
            .requireMemory(bytes.length, MemoryGroupManger.SHUFFLE);

        view2.getWriter().write(bytes);
        view2.getWriter().write(bytes);
        view2.getWriter().write(bytes);

        bytes1 = new byte[bytes.length + 10];

        MemoryViewReader reader = view2.getReader();
        reader.skip(bytes.length * 2);
        reader.read(bytes1);

        byte[] bytes2 = Arrays.copyOf(bytes1, bytes.length);
        Assert.assertEquals(sb.toString(), new String(bytes2));

        byte[] bytes3 = Arrays.copyOfRange(bytes1, bytes.length, bytes.length + 10);

        Assert.assertEquals(bytes3, new byte[10]);

        view1.close();
        view.close();
        view2.close();
    }

    @Test
    public void testReuseWithLoop() {
        Map<String, String> config = new HashMap<>();
        config.put(MemoryConfigKeys.OFF_HEAP_MEMORY_SIZE_MB.getKey(), "320");
        MemoryManager manager = MemoryManager.build(new Configuration(config));

        long sum = 0;
        long start = System.nanoTime();
        for (int i = 0; i < 1000; i++) {
            testReuse();
        }

        sum += (System.nanoTime() - start);

        System.out.println("reuse:" + sum);
    }

    @Test
    public void testReNewWithLoop() {
        Map<String, String> config = new HashMap<>();
        config.put(MemoryConfigKeys.OFF_HEAP_MEMORY_SIZE_MB.getKey(), "320");
        MemoryManager manager = MemoryManager.build(new Configuration(config));
        long sum = 0;
        long start = System.nanoTime();
        for (int i = 0; i < 1000; i++) {
            testReNew();
        }
        sum += (System.nanoTime() - start);

        System.out.println("renew:" + sum);

    }

    @Test
    public void testWriteBuffer() throws IOException {

        Map<String, String> config = new HashMap<>();
        config.put(MemoryConfigKeys.OFF_HEAP_MEMORY_SIZE_MB.getKey(), "320");
        MemoryManager manager = MemoryManager.build(new Configuration(config));
        MemoryView view = manager.requireMemory(2049, MemoryGroupManger.SHUFFLE);

        ByteBuffer buffer = ByteBuffer.allocateDirect(1025);
        byte[] a = new byte[999];
        for (int i = 0; i < a.length; i++) {
            a[i] = 1;
        }
        buffer.put(a);
        buffer.flip();

        view.getWriter().write(buffer, a.length);

        byte[] read = new byte[a.length];
        view.getReader().read(read);
        Assert.assertEquals(a, read);

        view.close();

        buffer.flip();
        view = manager.requireMemory(512, MemoryGroupManger.SHUFFLE);
        view.getWriter(512).write(buffer, a.length);

        read = new byte[a.length];
        view.getReader().read(read);
        Assert.assertEquals(read, a);
    }

    @Test
    public void testRollback() {
        Map<String, String> config = new HashMap<>();
        config.put(MemoryConfigKeys.OFF_HEAP_MEMORY_SIZE_MB.getKey(), "320");
        MemoryManager manager = MemoryManager.build(new Configuration(config));

        MemoryView view1 = MemoryManager.getInstance()
            .requireMemory(100, MemoryGroupManger.SHUFFLE);

        MemoryViewWriter viewWriter = view1.getWriter(100);

        byte[] bytes = new byte[100];
        Arrays.fill(bytes, (byte) 1);
        viewWriter.write(bytes);
        viewWriter.position(100);

        bytes = new byte[101];
        viewWriter.write(bytes);

        viewWriter.position(101);

        Assert.assertTrue(view1.contentSize() == 101);
        view1.getReader().read(bytes);
        Assert.assertEquals(bytes[100], 0);

        viewWriter.write(bytes);
        Assert.assertTrue(view1.contentSize() == 202);
        bytes = new byte[202];
        view1.getReader().read(bytes);
        Assert.assertEquals(bytes[100], 0);
        Assert.assertEquals(bytes[201], 0);

        viewWriter.position(200);
        Assert.assertTrue(view1.contentSize() == 200);
        bytes = new byte[201];
        view1.getReader().read(bytes);
        Assert.assertEquals(bytes[199], 1);
        Assert.assertEquals(bytes[200], 0);

        viewWriter.position(0);
        Assert.assertTrue(view1.contentSize() == 0);

    }

    @Test
    public void testAllocate() throws IOException {
        Map<String, String> config = new HashMap<>();
        config.put(MemoryConfigKeys.OFF_HEAP_MEMORY_SIZE_MB.getKey(), "1");
        config.put(MemoryConfigKeys.MAX_DIRECT_MEMORY_SIZE.getKey(), "" + 1024 * 1024);
        config.put(MemoryConfigKeys.MEMORY_POOL_SIZE.getKey(), "1");
        MemoryManager manager = MemoryManager.build(new Configuration(config));
        MemoryView view = manager.requireMemory(1024 * 1024, MemoryGroupManger.SHUFFLE);
        int pos = 0;
        for (int i = 0; i < 1000000; i++) {
            try {
                int len = new Random(1000).nextInt(1000);
                byte[] bytes = new byte[len];
                Arrays.fill(bytes, (byte) 1);
                pos = view.contentSize;
                view.getWriter().write(bytes);
            } catch (Throwable t) {
                Assert.assertTrue(view.contentSize() == 16 * 1024 * 1024);
                view.getWriter().position(pos);
                ByteArrayOutputStream outputStream = new ByteArrayOutputStream(16 * 1024 * 1024);
                MemoryViewReader reader = view.getReader();
                int len = reader.read(outputStream);
                Assert.assertEquals(len, view.contentSize);
                System.out.println(len);
                System.out.println(view.contentSize());

                Assert.assertEquals(outputStream.size(), len);
                byte[] bytes = new byte[len];
                Arrays.fill(bytes, (byte) 1);
                Assert.assertEquals(outputStream.toByteArray(), bytes);
                break;
            }
        }

    }

    @Test
    public void testWriteOutOfBounds() throws NoSuchFieldException, IllegalAccessException {

        MemoryView view = new MemoryView(Lists.newArrayList(new ByteBuf()),
            MemoryGroupManger.SHUFFLE);
        Field field = view.getClass().getDeclaredField("contentSize");
        field.setAccessible(true);
        field.setInt(view, Integer.MAX_VALUE);
        boolean hasExp = false;
        try {
            view.getWriter().write(1);
        } catch (IllegalArgumentException e) {
            hasExp = true;
        }

        Assert.assertTrue(hasExp);
        hasExp = false;

        try {
            view.getWriter().write(new byte[10]);
        } catch (IllegalArgumentException e) {
            hasExp = true;
        }

        Assert.assertTrue(hasExp);

        hasExp = false;

        try {
            view.getWriter().write(new byte[10], 0, 9);
        } catch (IllegalArgumentException e) {
            hasExp = true;
        }

        Assert.assertTrue(hasExp);

        hasExp = false;

        try {
            view.getWriter().write(ByteBuffer.wrap(new byte[10]), 10);
        } catch (IllegalArgumentException | IOException e) {
            hasExp = true;
        }

        Assert.assertTrue(hasExp);

        hasExp = false;

        try {
            ByteArrayInputStream inputStream = new ByteArrayInputStream(new byte[10]);
            view.getWriter().write(inputStream, 10);
        } catch (IllegalArgumentException | IOException e) {
            hasExp = true;
        }

        Assert.assertTrue(hasExp);

    }

    @Test
    public void testReader() throws IOException {
        Map<String, String> config = new HashMap<>();
        config.put(MemoryConfigKeys.OFF_HEAP_MEMORY_SIZE_MB.getKey(), "32");
        config.put(MemoryConfigKeys.MAX_DIRECT_MEMORY_SIZE.getKey(), "" + 32 * 1024 * 1024);
        config.put(MemoryConfigKeys.MEMORY_POOL_SIZE.getKey(), "1");
        MemoryManager manager = MemoryManager.build(new Configuration(config));

        MemoryView view = manager.requireMemory((int) MemoryUtils.KB, MemoryGroupManger.DEFAULT);

        byte[] bytes = new byte[1024];
        Arrays.fill(bytes, (byte) 1);
        view.getWriter().write(bytes);
        view.getWriter().write(bytes);
        view.getWriter().write(bytes);
        bytes = new byte[128];
        Arrays.fill(bytes, (byte) 0);
        view.getWriter().write(bytes);

        MemoryViewReader reader = view.getReader();
        Assert.assertEquals(reader.readPos(), 0);

        reader.read(new byte[1025]);
        Assert.assertEquals(reader.readPos(), 1025);

        reader.read();
        Assert.assertEquals(reader.readPos(), 1026);

        reader.skip(1024);
        Assert.assertEquals(reader.readPos(), 1026 + 1024);

        reader.read(new ByteArrayOutputStream());

        Assert.assertEquals(reader.readPos(), 3 * 1024 + 128);

        Assert.assertFalse(reader.hasNext());

        boolean hasExp = false;
        try {
            reader.read();
        } catch (BufferUnderflowException e) {
            hasExp = true;
        }
        Assert.assertTrue(hasExp);

    }
}
