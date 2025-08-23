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

package org.apache.geaflow.example.function;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import org.apache.commons.io.FileUtils;
import org.apache.geaflow.api.context.RuntimeContext;
import org.apache.geaflow.api.function.RichFunction;
import org.apache.geaflow.api.function.io.SinkFunction;
import org.apache.geaflow.common.config.ConfigKey;
import org.apache.geaflow.common.exception.GeaflowRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class FileSink<OUT> extends RichFunction implements SinkFunction<OUT> {

    private static final Logger LOGGER = LoggerFactory.getLogger(FileSink.class);

    public static final String OUTPUT_DIR = "output.dir";
    public static final String FILE_OUTPUT_APPEND_ENABLE = "file.append.enable";

    private File file;

    public FileSink() {
    }

    @Override
    public void open(RuntimeContext runtimeContext) {
        String filePath = String.format("%s/result_%s",
            runtimeContext.getConfiguration().getString(OUTPUT_DIR), runtimeContext.getTaskArgs().getTaskIndex());
        LOGGER.info("sink file name {}", filePath);
        boolean append = runtimeContext.getConfiguration().getBoolean(new ConfigKey(FILE_OUTPUT_APPEND_ENABLE, true));
        file = new File(filePath);
        try {
            if (!append && file.exists()) {
                try {
                    FileUtils.forceDelete(file);
                } catch (Exception e) {
                    // ignore
                }
            }

            if (!file.exists()) {
                if (!file.getParentFile().exists()) {
                    file.getParentFile().mkdirs();
                }
                file.createNewFile();
            }
        } catch (IOException e) {
            throw new GeaflowRuntimeException(e);
        }
    }

    @Override
    public void close() {

    }

    @Override
    public void write(OUT out) throws Exception {
        try {
            FileUtils.write(file, out + "\n", Charset.defaultCharset(), true);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
