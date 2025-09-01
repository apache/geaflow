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
import io.netty.channel.Channel;
import io.netty.util.internal.PlatformDependent;
import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.ThreadFactory;
import org.apache.geaflow.common.utils.ThreadUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NettyUtils {

    private static final Logger LOGGER = LoggerFactory.getLogger(NettyUtils.class);

    public static ThreadFactory getNamedThreadFactory(String name) {
        return ThreadUtil.namedThreadFactory(true, name);
    }

    /**
     * Returns the remote address on the channel or "&lt;unknown remote&gt;" if none exists.
     */
    public static String getRemoteAddress(Channel channel) {
        if (channel != null && channel.remoteAddress() != null) {
            return channel.remoteAddress().toString();
        }
        return "<unknown remote>";
    }

    /**
     * Create a pooled ByteBuf allocator.
     */
    public static PooledByteBufAllocator createPooledByteBufAllocator(
        boolean allowDirectBufs,
        boolean allowCache,
        int numCores) {
        if (numCores == 0) {
            numCores = Runtime.getRuntime().availableProcessors();
        }
        boolean preferDirect = allowDirectBufs && PlatformDependent.directBufferPreferred();
        LOGGER.info("create a PooledByteBufAllocator: preferDirect={}, allowCache={}",
            preferDirect, allowCache);
        return new PooledByteBufAllocator(
            preferDirect,
            Math.min(PooledByteBufAllocator.defaultNumHeapArena(), preferDirect ? 0 : numCores),
            Math.min(PooledByteBufAllocator.defaultNumDirectArena(), preferDirect ? numCores : 0),
            PooledByteBufAllocator.defaultPageSize(),
            PooledByteBufAllocator.defaultMaxOrder(),
            allowCache ? PooledByteBufAllocator.defaultTinyCacheSize() : 0,
            allowCache ? PooledByteBufAllocator.defaultSmallCacheSize() : 0,
            allowCache ? PooledByteBufAllocator.defaultNormalCacheSize() : 0,
            allowCache ? PooledByteBufAllocator.defaultUseCacheForAllThreads() : false
        );
    }

    /**
     * Closes the given object, ignoring IOExceptions.
     */
    public static void closeQuietly(Closeable closeable) {
        try {
            if (closeable != null) {
                closeable.close();
            }
        } catch (IOException e) {
            LOGGER.error("IOException should not have been thrown.", e);
        }
    }

}
