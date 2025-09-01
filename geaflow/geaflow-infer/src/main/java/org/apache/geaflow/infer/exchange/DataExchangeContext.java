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

package org.apache.geaflow.infer.exchange;

import static org.apache.geaflow.common.config.keys.FrameworkConfigKeys.INFER_ENV_SHARE_MEMORY_QUEUE_SIZE;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import org.apache.commons.io.FileUtils;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.common.exception.GeaflowRuntimeException;
import org.apache.geaflow.infer.util.InferFileUtils;

public class DataExchangeContext implements Closeable {

    private static final String FILE_KEY_PREFIX = "queue-";

    private static final int INT_SIZE = 4;

    private static final String MMAP_KEY_PREFIX = "queue://";
    private static final String SHARE_MEMORY_DIR = "/infer_data";
    private static final String KEY_SEPARATOR = ":";

    private static final String MMAP_INPUT_KEY_SUFFIX = ".input";
    private static final String MMAP_OUTPUT_KEY_SUFFIX = ".output";
    private final File localDirectory;
    private final long queueEndpoint;
    private final DataExchangeQueue receiveQueue;
    private final DataExchangeQueue sendQueue;

    private final File receiveQueueFile;
    private final File sendQueueFile;
    private String receivePath;
    private String sendPath;

    public DataExchangeContext(Configuration config) {
        this.localDirectory = new File(InferFileUtils.getInferDirectory(config) + SHARE_MEMORY_DIR);
        this.queueEndpoint = UnSafeUtils.UNSAFE.allocateMemory(INT_SIZE);
        UnSafeUtils.UNSAFE.setMemory(queueEndpoint, INT_SIZE, (byte) 0);
        this.receiveQueueFile = createTempFile(FILE_KEY_PREFIX, MMAP_INPUT_KEY_SUFFIX);
        this.sendQueueFile = createTempFile(FILE_KEY_PREFIX, MMAP_OUTPUT_KEY_SUFFIX);
        this.receivePath = receiveQueueFile.getAbsolutePath();
        this.sendPath = sendQueueFile.getAbsolutePath();
        int queueCapacity = config.getInteger(INFER_ENV_SHARE_MEMORY_QUEUE_SIZE);
        this.receiveQueue = new DataExchangeQueue(receivePath, queueCapacity, true);
        this.sendQueue = new DataExchangeQueue(sendPath, queueCapacity, true);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> UnSafeUtils.UNSAFE.freeMemory(queueEndpoint)));
    }

    public String getReceiveQueueKey() {
        return MMAP_KEY_PREFIX + receivePath + KEY_SEPARATOR + receiveQueue.getMemoryMapSize();
    }

    public String getSendQueueKey() {
        return MMAP_KEY_PREFIX + sendPath + KEY_SEPARATOR + sendQueue.getMemoryMapSize();
    }

    @Override
    public synchronized void close() throws IOException {
        if (receiveQueue != null) {
            receiveQueue.close();
        }
        if (sendQueue != null) {
            sendQueue.close();
        }
        if (receiveQueueFile != null) {
            receiveQueueFile.delete();
        }
        if (sendQueueFile != null) {
            sendQueueFile.delete();
        }
        UnSafeUtils.UNSAFE.freeMemory(this.queueEndpoint);
        FileUtils.deleteQuietly(localDirectory);
    }

    public DataExchangeQueue getReceiveQueue() {
        return receiveQueue;
    }

    public DataExchangeQueue getSendQueue() {
        return sendQueue;
    }

    private File createTempFile(String prefix, String suffix) {
        try {
            if (!localDirectory.exists()) {
                InferFileUtils.forceMkdir(localDirectory);
            }
            return File.createTempFile(prefix, suffix, localDirectory);
        } catch (IOException e) {
            throw new GeaflowRuntimeException("create temp file on infer directory failed ", e);
        }
    }
}
