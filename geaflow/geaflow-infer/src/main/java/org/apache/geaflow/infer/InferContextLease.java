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

package org.apache.geaflow.infer;

import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.geaflow.common.exception.GeaflowRuntimeException;

public class InferContextLease<OUT> implements AutoCloseable {

    private final InferContextPool.PoolEntry poolEntry;
    private final InferContext<OUT> inferContext;
    private final AtomicBoolean closed = new AtomicBoolean(false);

    InferContextLease(InferContextPool.PoolEntry poolEntry, InferContext<OUT> inferContext) {
        this.poolEntry = poolEntry;
        this.inferContext = inferContext;
    }

    public OUT infer(Object... feature) throws Exception {
        if (closed.get()) {
            throw new GeaflowRuntimeException("infer context lease already closed");
        }
        return inferContext.infer(feature);
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            poolEntry.release(inferContext, inferContext.isBroken());
        }
    }
}
