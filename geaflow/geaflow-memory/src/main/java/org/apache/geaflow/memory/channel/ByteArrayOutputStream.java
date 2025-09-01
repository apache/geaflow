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

package org.apache.geaflow.memory.channel;

import java.io.IOException;
import java.io.OutputStream;
import org.apache.geaflow.memory.MemoryView;
import org.apache.geaflow.memory.MemoryViewWriter;

public class ByteArrayOutputStream extends OutputStream {

    private MemoryView view = null;
    private MemoryViewWriter writer = null;

    public ByteArrayOutputStream(MemoryView view) {
        if (view != null) {
            this.view = view;
            this.writer = view.getWriter();
        }
    }

    @Override
    public void write(int b) throws IOException {
        writer.write(b);
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        writer.write(b, off, len);
    }

    @Override
    public void flush() throws IOException {
        super.flush();
    }

    @Override
    public void close() throws IOException {
        super.close();
    }

    public MemoryView getView() {
        return view;
    }
}
