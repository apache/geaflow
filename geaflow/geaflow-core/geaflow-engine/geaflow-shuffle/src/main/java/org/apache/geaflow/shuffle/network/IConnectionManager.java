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

package org.apache.geaflow.shuffle.network;

import io.netty.buffer.PooledByteBufAllocator;
import java.io.IOException;
import java.util.concurrent.ExecutorService;
import org.apache.geaflow.common.shuffle.ShuffleAddress;
import org.apache.geaflow.shuffle.config.ShuffleConfig;

public interface IConnectionManager {

    /**
     * Get the shuffle config.
     *
     * @return shuffle config.
     */
    ShuffleConfig getShuffleConfig();

    /**
     * Client buffer.
     *
     * @return buffer.
     */
    PooledByteBufAllocator getClientBufAllocator();

    /**
     * Server buffer.
     *
     * @return buffer.
     */
    PooledByteBufAllocator getServerBufAllocator();

    /**
     * Get the shuffle address.
     *
     * @return shuffle address.
     */
    ShuffleAddress getShuffleAddress();

    /**
     * Close connection manager.
     *
     * @throws IOException io exception.
     */
    void close() throws IOException;

    /**
     * Get the thread pool to async callback action.
     *
     * @return thread pool.
     */
    ExecutorService getExecutor();

}
