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

package org.apache.geaflow.memory.jmh;

import com.google.common.collect.Maps;
import java.util.HashMap;
import java.util.concurrent.TimeUnit;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.memory.MemoryGroupManger;
import org.apache.geaflow.memory.MemoryManager;
import org.apache.geaflow.memory.MemoryView;
import org.apache.geaflow.memory.MemoryViewWriter;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(1)
@State(Scope.Benchmark)
public class GeaflowMemoryViewWriterBenchmark {


    @Param({"1", "32", "64", "128", "512", "1024", "2048", "4096", "8192", "10240"})
    public int memBytes;

    private MemoryView view;
    private MemoryViewWriter writer;
    private int spanSize;

    @Benchmark
    public void write() {
        HashMap<String, String> config = Maps.newHashMap();
        config.put("max.direct.memory.size.mb", "512");
        config.put("off.heap.memory.chunkSize.MB", "16");
        MemoryManager.build(new Configuration(config));
        view = MemoryManager.getInstance().requireMemory(memBytes, MemoryGroupManger.DEFAULT);

        spanSize = memBytes / 2 == 0 ? 1 : memBytes / 2;
        writer = view.getWriter(spanSize);

        for (int i = 0; i < memBytes; i++) {
            writer.write(new byte[spanSize]);
        }
        view.close();
    }

    @TearDown
    public void finish() {
        MemoryManager.getInstance().dispose();
    }
}
