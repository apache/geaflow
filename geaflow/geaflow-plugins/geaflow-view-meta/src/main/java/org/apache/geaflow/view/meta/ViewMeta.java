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

package org.apache.geaflow.view.meta;

import com.google.common.base.Preconditions;
import com.google.protobuf.ByteString;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.Map;
import org.apache.geaflow.file.FileInfo;
import org.apache.geaflow.file.IPersistentIO;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ViewMeta {

    private static final Logger LOGGER = LoggerFactory.getLogger(ViewMeta.class);

    private static final String BACKUP_SUFFIX = ".bak";
    private static final String TMP_SUFFIX = ".tmp";
    private final Map<String, ByteString> innerKV = new HashMap<>();
    private final String filePath;
    private final IPersistentIO persistIO;
    private long fileModifyTS;

    public ViewMeta(String filePath, IPersistentIO persistentIO) throws IOException {
        this.filePath = filePath;
        this.persistIO = persistentIO;
        if (persistentIO.exists(new Path(filePath))) {
            readFile(filePath);
        } else if (persistIO.exists(new Path(filePath + BACKUP_SUFFIX))) {
            readFile(filePath + BACKUP_SUFFIX);
        }
    }

    private void readFile(String strFilePath) throws IOException {
        FileInfo info = persistIO.getFileInfo(new Path(strFilePath));
        InputStream stream = persistIO.open(new Path(strFilePath));
        byte[] content = new byte[(int) info.getLength()];
        stream.read(content);
        stream.close();
        ViewMetaPb.ViewMeta viewMeta = ViewMetaPb.ViewMeta.parseFrom(content);
        this.innerKV.putAll(viewMeta.getKvInfoMap());
        this.fileModifyTS = info.getModificationTime();
    }

    public void tryRefresh() throws IOException {
        if (!persistIO.exists(new Path(filePath))) {
            return;
        }
        long currentModifyTime = this.persistIO.getFileInfo(new Path(filePath)).getModificationTime();
        if (currentModifyTime > fileModifyTS) {
            LOGGER.info("refresh {}", filePath);
            readFile(filePath);
        } else {
            LOGGER.info("last {} now {}", fileModifyTS, currentModifyTime);
        }
    }

    public Map<String, ByteString> getKVMap() {
        return innerKV;
    }

    public byte[] toBinary() {
        return ViewMetaPb.ViewMeta
            .newBuilder()
            .putAllKvInfo(innerKV)
            .build()
            .toByteArray();
    }

    public void archive() throws IOException {
        final long start = System.currentTimeMillis();
        // fo, keep history
        if (persistIO.exists(new Path(filePath))) {
            persistIO.delete(new Path(filePath + BACKUP_SUFFIX), false);
            boolean res = persistIO.renameFile(new Path(filePath), new Path(filePath + BACKUP_SUFFIX));
            Preconditions.checkArgument(res, "renameFile fail " + filePath);
        }
        // fo, protect filePath.
        final java.nio.file.Path path = Files.createTempFile("tmp", TMP_SUFFIX);
        Files.write(path, toBinary());
        persistIO.copyFromLocalFile(new Path(path.toString()), new Path(filePath + TMP_SUFFIX));
        Files.deleteIfExists(path);

        // clean.
        boolean res = persistIO.renameFile(new Path(filePath + TMP_SUFFIX), new Path(filePath));
        Preconditions.checkArgument(res, "renameFile fail " + filePath + TMP_SUFFIX);
        if (persistIO.exists(new Path(filePath + BACKUP_SUFFIX))) {
            persistIO.delete(new Path(filePath + BACKUP_SUFFIX), false);
        }
        LOGGER.info("save {} cost {}ms", filePath, System.currentTimeMillis() - start);
    }
}
