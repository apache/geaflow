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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import org.apache.geaflow.common.utils.MemoryUtils;
import org.testng.Assert;
import org.testng.annotations.Test;

public class DirectMemoryTest {

    @Test
    public void test() {
        List<String> args = new ArrayList<>();
        args.add("-XX:MaxDirectMemorySize=2G");
        long s = DirectMemory.maxDirectMemoryFromJVMOption(args);
        Assert.assertEquals(MemoryUtils.humanReadableByteCount(s), "2.00GB");

        ByteBuffer bf = DirectMemory.allocateDirectNoCleaner(20);
        long addr = DirectMemory.directBufferAddress(bf);
        DirectMemory.setMemory(addr, 20, (byte) 0);

        DirectMemory.putInt(addr, 100);
        DirectMemory.putLong(addr + 4, 1000L);
        DirectMemory.putShort(addr + 12, (short) 10);
        DirectMemory.putByte(addr + 14, (byte) 1);

        Assert.assertEquals(DirectMemory.getInt(addr), 100);
        Assert.assertEquals(DirectMemory.getLong(addr + 4), 1000L);
        Assert.assertEquals(DirectMemory.getShort(addr + 12), 10);
        Assert.assertEquals(DirectMemory.getByte(addr + 14), 1);
        Assert.assertEquals(DirectMemory.getByte(addr + 15), 0);

        ByteBuffer bf2 = DirectMemory.allocateDirectNoCleaner(20);
        long addr2 = DirectMemory.directBufferAddress(bf2);
        DirectMemory.copyMemory(addr, addr2, 20);

        Assert.assertEquals(DirectMemory.getInt(addr2), 100);
        Assert.assertEquals(DirectMemory.getLong(addr2 + 4), 1000L);
        Assert.assertEquals(DirectMemory.getShort(addr2 + 12), 10);
        Assert.assertEquals(DirectMemory.getByte(addr2 + 14), 1);
        Assert.assertEquals(DirectMemory.getByte(addr2 + 15), 0);

        ByteBuffer[] bfs = DirectMemory.splitBuffer(bf, 10);
        Assert.assertEquals(bfs.length, 2);
        ByteBuffer bf3 = DirectMemory.mergeBuffer(bfs[0], bfs[1]);
        long addr3 = DirectMemory.directBufferAddress(bf3);
        Assert.assertEquals(addr, addr3);

        DirectMemory.freeDirectBuffer(bf2);
        DirectMemory.freeDirectBuffer(bf3);
    }
}
