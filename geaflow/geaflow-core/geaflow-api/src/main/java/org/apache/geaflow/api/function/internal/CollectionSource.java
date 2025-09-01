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

package org.apache.geaflow.api.function.internal;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.geaflow.api.context.RuntimeContext;
import org.apache.geaflow.api.function.RichFunction;
import org.apache.geaflow.api.function.io.SourceFunction;
import org.apache.geaflow.api.window.IWindow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CollectionSource<OUT> extends RichFunction implements SourceFunction<OUT> {

    private static final Logger LOGGER = LoggerFactory.getLogger(CollectionSource.class);

    private int batchSize;
    private List<OUT> records;
    private static Map<Integer, Integer> readPosMap = new ConcurrentHashMap<>();

    private transient RuntimeContext runtimeContext;

    public CollectionSource(Collection<OUT> records, int batchSize) {
        this.records = new ArrayList<>(records);
        this.batchSize = batchSize;
    }

    public CollectionSource(Collection<OUT> records) {
        this(records, 2);
    }

    public CollectionSource(OUT... collections) {
        this(Arrays.asList(collections));
    }

    @Override
    public void open(RuntimeContext runtimeContext) {
        this.runtimeContext = runtimeContext;
    }

    @Override
    public void init(int parallel, int index) {
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
        String taskName = runtimeContext.getTaskArgs().getTaskName();
        int taskId = runtimeContext.getTaskArgs().getTaskId();
        int readPos = readPosMap.getOrDefault(taskId, 0);
        LOGGER.info("taskName:{} fetch batchId:{} readPos:{}", taskName, window.windowId(), readPos);
        while (readPos < records.size()) {
            OUT out = records.get(readPos);
            if (window.assignWindow(out) == window.windowId() && ctx.collect(out)) {
                readPos++;
            } else {
                break;
            }
        }
        LOGGER.info("taskName:{} save batchId:{} readPos:{}", taskName, window.windowId(), readPos);
        readPosMap.put(taskId, readPos);
        return readPos < records.size();
    }

    @Override
    public void close() {
    }
}
