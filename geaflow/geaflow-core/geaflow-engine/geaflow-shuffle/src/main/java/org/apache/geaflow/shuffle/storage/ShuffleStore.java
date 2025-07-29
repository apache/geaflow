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

package org.apache.geaflow.shuffle.storage;

import java.io.InputStream;
import java.io.OutputStream;
import org.apache.geaflow.common.exception.GeaflowRuntimeException;
import org.apache.geaflow.common.shuffle.StorageLevel;
import org.apache.geaflow.shuffle.config.ShuffleConfig;

public interface ShuffleStore {

    /**
     * Get file path.
     *
     * @param fileName file name
     * @return file path.
     */
    String getFilePath(String fileName);

    /**
     * Get input stream by filePath.
     *
     * @param path file path.
     * @return file input stream.
     */
    InputStream getInputStream(String path);

    /**
     * Get output stream by filePath.
     *
     * @param path file path
     * @return file output stream.
     */
    OutputStream getOutputStream(String path);

    static ShuffleStore getShuffleStore(ShuffleConfig shuffleConfig) {
        StorageLevel storageLevel = shuffleConfig.getStorageLevel();
        if (storageLevel == StorageLevel.DISK || storageLevel == StorageLevel.MEMORY_AND_DISK) {
            return new LocalShuffleStore(shuffleConfig);
        } else {
            throw new GeaflowRuntimeException("unsupported shuffle level: " + storageLevel);
        }
    }

}
