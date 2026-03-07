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

import net.jpountz.lz4.LZ4BlockInputStream;
import net.jpountz.lz4.LZ4BlockOutputStream;
import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4FastDecompressor;
import org.apache.geaflow.memory.MemoryGroupManger;
import org.apache.geaflow.memory.MemoryManager;
import org.apache.geaflow.memory.MemoryView;
import org.apache.geaflow.memory.MemoryViewWriter;
import org.apache.geaflow.memory.channel.ByteArrayInputStream;
import org.apache.geaflow.memory.channel.ByteArrayOutputStream;

public class Lz4 {

    private static final int BLOCK_SIZE = 1024 * 8;
    private static final LZ4Compressor COMPRESSOR = LZ4Factory.safeInstance().fastCompressor();
    private static final LZ4FastDecompressor DECOMPRESSOR = LZ4Factory.safeInstance().fastDecompressor();

    public static MemoryView compress(MemoryView view) {
        MemoryView v =
            MemoryManager.getInstance().requireMemory(view.contentSize() / 2, MemoryGroupManger.STATE);
        ByteArrayOutputStream baos = new ByteArrayOutputStream(v);

        LZ4BlockOutputStream compressedOutput = new LZ4BlockOutputStream(
            baos, BLOCK_SIZE, COMPRESSOR);
        try {
            compressedOutput.write(view.toArray(), 0, view.contentSize());
            compressedOutput.close();
        } catch (Throwable throwable) {
            throw new RuntimeException("compress fail", throwable);
        }
        return baos.getView();
    }

    public static MemoryView uncompress(MemoryView view) {
        return uncompress(view, view.contentSize() * 2);
    }

    public static MemoryView uncompress(MemoryView view, int initSize) {
        MemoryView v = MemoryManager.getInstance().requireMemory(initSize, MemoryGroupManger.STATE);
        MemoryViewWriter writer = v.getWriter();
        int count;
        byte[] buffer = new byte[BLOCK_SIZE];

        try (LZ4BlockInputStream lzis = new LZ4BlockInputStream(
            new ByteArrayInputStream(view), DECOMPRESSOR)) {
            while ((count = lzis.read(buffer)) != -1) {
                writer.write(buffer, 0, count);
            }
        } catch (Throwable throwable) {
            throw new RuntimeException("uncompress fail", throwable);
        }
        return v;
    }
}
