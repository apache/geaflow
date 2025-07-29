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

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import org.apache.spark.SparkConf;
import org.apache.spark.memory.MemoryConsumer;
import org.apache.spark.memory.MemoryMode;
import org.apache.spark.memory.TaskMemoryManager;
import org.apache.spark.memory.UnifiedMemoryManager;
import org.apache.spark.unsafe.memory.MemoryBlock;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.Warmup;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(1)
public class AllocationSparkBenchmark extends JMHParameter {

    private TaskMemoryManager memoryManager;
    private MemoryConsumer consumer;

    @Setup
    public void setUp() {
        SparkConf conf = new SparkConf();
        conf.set("spark.memory.offHeap.enabled", "true");
        conf.set("spark.memory.offHeap.size", "1342177280");
        memoryManager = new TaskMemoryManager(
            new UnifiedMemoryManager(conf, 134217728 * 20L, 1342177280L, 1), 1);
        consumer = new LocalMemoryConsumer(memoryManager, MemoryMode.OFF_HEAP);
    }

    @Benchmark
    public void allocateAndFree() {
        MemoryBlock block = memoryManager.allocatePage(allocateBytes, consumer);
        memoryManager.freePage(block, consumer);
    }

    public void finish() {
        memoryManager.cleanUpAllAllocatedMemory();
    }


    static class LocalMemoryConsumer extends MemoryConsumer {

        protected LocalMemoryConsumer(TaskMemoryManager taskMemoryManager, long pageSize,
                                      MemoryMode mode) {
            super(taskMemoryManager, pageSize, mode);
        }

        protected LocalMemoryConsumer(TaskMemoryManager taskMemoryManager, MemoryMode mode) {
            super(taskMemoryManager, mode);
        }

        @Override
        public long spill(long size, MemoryConsumer trigger) throws IOException {
            return 0;
        }
    }

}
