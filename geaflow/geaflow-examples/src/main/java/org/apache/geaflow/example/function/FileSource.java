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

import com.google.common.io.Resources;
import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.apache.geaflow.api.context.RuntimeContext;
import org.apache.geaflow.api.function.RichFunction;
import org.apache.geaflow.api.function.io.SourceFunction;
import org.apache.geaflow.api.window.IWindow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileSource<OUT> extends RichFunction implements SourceFunction<OUT> {

    private static final Logger LOGGER = LoggerFactory.getLogger(FileSource.class);

    protected String filePath;
    protected List<OUT> records;
    protected Integer readPos = null;
    protected FileLineParser<OUT> parser;
    protected transient RuntimeContext runtimeContext;

    public FileSource(String filePath, FileLineParser<OUT> parser) {
        this.filePath = filePath;
        this.parser = parser;
    }

    @Override
    public void open(RuntimeContext runtimeContext) {
        this.runtimeContext = runtimeContext;
    }

    @Override
    public void init(int parallel, int index) {
        this.records = readFileLines(filePath);
        if (parallel != 1) {
            List<OUT> allRecords = records;
            records = new ArrayList<>();
            for (int i = 0; i < allRecords.size(); i++) {
                if (i % parallel == index) {
                    records.add(allRecords.get(i));
                }
            }
        }
    }

    @Override
    public boolean fetch(IWindow<OUT> window, SourceContext<OUT> ctx) throws Exception {
        LOGGER.info("collection source fetch taskId:{}, batchId:{}, start readPos {}, totalSize {}",
            runtimeContext.getTaskArgs().getTaskId(), window.windowId(), readPos, records.size());
        if (readPos == null) {
            readPos = 0;
        }
        while (readPos < records.size()) {
            OUT out = records.get(readPos);
            long windowId = window.assignWindow(out);
            if (window.windowId() == windowId) {
                ctx.collect(out);
                readPos++;
            } else {
                break;
            }
        }
        boolean result = false;
        if (readPos < records.size()) {
            result = true;
        }
        LOGGER.info("collection source fetch batchId:{}, current readPos {}, result {}",
            window.windowId(), readPos, result);
        return result;
    }

    @Override
    public void close() {
    }

    private List<OUT> readFileLines(String filePath) {
        try {
            List<String> lines = Resources.readLines(Resources.getResource(filePath),
                Charset.defaultCharset());
            List<OUT> result = new ArrayList<>();

            for (String line : lines) {
                if (StringUtils.isBlank(line)) {
                    continue;
                }
                Collection<OUT> collection = parser.parse(line);
                result.addAll(collection);
            }
            return result;
        } catch (IOException e) {
            throw new RuntimeException("error in read resource file: " + filePath, e);
        }
    }

    public interface FileLineParser<OUT> extends Serializable {
        Collection<OUT> parse(String line);
    }
}
