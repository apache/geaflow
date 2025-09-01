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
import org.apache.geaflow.common.exception.GeaflowRuntimeException;
import org.apache.geaflow.memory.MemoryGroupManger;
import org.apache.geaflow.memory.MemoryManager;
import org.apache.geaflow.memory.MemoryView;
import org.apache.geaflow.memory.MemoryViewReader;
import org.apache.geaflow.memory.MemoryViewWriter;
import org.apache.geaflow.memory.channel.ByteArrayInputStream;
import org.apache.geaflow.memory.channel.ByteArrayOutputStream;
import org.xerial.snappy.SnappyFramedInputStream;
import org.xerial.snappy.SnappyFramedOutputStream;

public class Snappy {

    private static final int BUFFER_SIZE = 1024 * 8;

    public static MemoryView compress(MemoryView view) throws IOException {
        MemoryView v =
            MemoryManager.getInstance().requireMemory(view.contentSize() / 2, MemoryGroupManger.STATE);
        ByteArrayOutputStream baos =
            new ByteArrayOutputStream(v);

        try (SnappyFramedOutputStream sos = new SnappyFramedOutputStream(baos)) {
            MemoryViewReader reader = view.getReader();
            byte[] buffer = new byte[BUFFER_SIZE];
            while (true) {
                int count = reader.read(buffer);
                if (count <= 0) {
                    break;
                }
                sos.write(buffer, 0, count);
            }
            sos.flush();
            return baos.getView();
        } catch (Exception ex) {
            throw new GeaflowRuntimeException("uncompress fail", ex);
        }
    }

    public static MemoryView uncompress(MemoryView view) {
        return uncompress(view, view.contentSize());
    }

    public static MemoryView uncompress(MemoryView view, int initSize) {
        byte[] buffer = new byte[BUFFER_SIZE];
        try (SnappyFramedInputStream sis =
                 new SnappyFramedInputStream(new ByteArrayInputStream(view))) {
            MemoryView v = MemoryManager.getInstance().requireMemory(initSize, MemoryGroupManger.STATE);
            MemoryViewWriter writer = v.getWriter();
            while (true) {
                int count = sis.read(buffer);
                if (count <= 0) {
                    break;
                }
                writer.write(buffer, 0, count);
            }
            return v;
        } catch (Exception ex) {
            throw new GeaflowRuntimeException("uncompress fail", ex);
        }
    }

}
