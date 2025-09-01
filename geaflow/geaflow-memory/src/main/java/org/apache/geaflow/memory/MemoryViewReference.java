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

import java.io.Closeable;
import java.util.concurrent.atomic.AtomicInteger;

public class MemoryViewReference implements Closeable {

    private MemoryView memoryView;
    private final AtomicInteger refCnt;

    public MemoryViewReference(MemoryView memoryView) {
        this.memoryView = memoryView;
        refCnt = new AtomicInteger(1);
    }

    public void incRef() {
        refCnt.addAndGet(1);
    }

    public boolean decRef() {
        if (refCnt.addAndGet(-1) <= 0) {
            close();
            return true;
        }
        return false;
    }

    public MemoryView getMemoryView() {
        return memoryView;
    }

    public int refCnt() {
        return refCnt.get();
    }

    @Override
    public void close() {
        if (memoryView == null) {
            return;
        }
        synchronized (this) {
            if (memoryView != null) {
                memoryView.close();
                memoryView = null;
            }
        }
    }
}
