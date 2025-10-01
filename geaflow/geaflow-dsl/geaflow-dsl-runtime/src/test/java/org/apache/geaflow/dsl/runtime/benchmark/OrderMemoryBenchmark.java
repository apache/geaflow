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

package org.apache.geaflow.dsl.runtime.benchmark;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import org.apache.geaflow.common.type.Types;
import org.apache.geaflow.dsl.common.data.Row;
import org.apache.geaflow.dsl.common.data.impl.ObjectRow;
import org.apache.geaflow.dsl.runtime.expression.Expression;
import org.apache.geaflow.dsl.runtime.expression.field.FieldExpression;
import org.apache.geaflow.dsl.runtime.function.table.OrderByFunction;
import org.apache.geaflow.dsl.runtime.function.table.OrderByHeapSort;
import org.apache.geaflow.dsl.runtime.function.table.OrderByRadixSort;
import org.apache.geaflow.dsl.runtime.function.table.OrderByTimSort;
import org.apache.geaflow.dsl.runtime.function.table.order.OrderByField;
import org.apache.geaflow.dsl.runtime.function.table.order.OrderByField.ORDER;
import org.apache.geaflow.dsl.runtime.function.table.order.SortInfo;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.profile.GCProfiler;
import org.openjdk.jmh.results.format.ResultFormatType;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

@BenchmarkMode(Mode.SingleShotTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
@Measurement(iterations = 10)
@Fork(1)
public class OrderMemoryBenchmark {
    
    @Param({"10000", "100000", "1000000"})
    private int dataSize;
    
    @Param({"100", "1000", "10000"})
    private int topN;
    
    private OrderByFunction orderByFunction;
    private List<Row> testData;
    private SortInfo sortInfo = new SortInfo();
    
    @Setup(Level.Trial)
    public void setup() {
        // Create sort expression
        Expression expression = new FieldExpression(0, Types.INTEGER);

        OrderByField orderByField = new OrderByField();
        orderByField.expression = expression;
        orderByField.order = ORDER.ASC;

        List<OrderByField> orderByFields = new ArrayList<>(1);
        orderByFields.add(orderByField);

        sortInfo.orderByFields = orderByFields;
        sortInfo.fetch = topN;
        
        // Generate test data
        testData = generateTestData();
    }
    
    private List<Row> generateTestData() {
        List<Row> data = new ArrayList<>(dataSize);
        Random random = new Random(42);
        
        for (int i = 0; i < dataSize; i++) {
            Object[] values = {random.nextInt(dataSize * 10)};
            data.add(ObjectRow.create(values));
        }
        
        return data;
    }
    
    @Benchmark
    public Iterable<Row> benchmarkHeapSortMemory() {
        // Create a copy of the input data to avoid state pollution
        List<Row> inputData = new ArrayList<>(testData);

        orderByFunction = new OrderByHeapSort(sortInfo);
        orderByFunction.open(null);

        for (int i = 0; i < dataSize; i++) {
            orderByFunction.process(inputData.get(i));
        }
        
        // Perform Top-N sorting
        return orderByFunction.finish();
    }

    @Benchmark
    public Iterable<Row> benchmarkRadixSortMemory() {
        List<Row> inputData = new ArrayList<>(testData);
        
        orderByFunction = new OrderByRadixSort(sortInfo);
        orderByFunction.open(null);

        for (int i = 0; i < dataSize; i++) {
            orderByFunction.process(inputData.get(i));
        }
        
        return orderByFunction.finish();
    }
    
    @Benchmark
    public Iterable<Row> benchmarkTimSortMemory() {
        List<Row> inputData = new ArrayList<>(testData);
        
        orderByFunction = new OrderByTimSort(sortInfo);
        orderByFunction.open(null);

        for (int i = 0; i < dataSize; i++) {
            orderByFunction.process(inputData.get(i));
        }
        
        return orderByFunction.finish();
    }
    
    public static void main(String[] args) throws RunnerException {
        // Run a verification first
        OrderMemoryBenchmark benchmark = new OrderMemoryBenchmark();
        benchmark.dataSize = 10;
        benchmark.topN = 10;
        benchmark.setup();
        Iterable<Row> heapResults = benchmark.benchmarkHeapSortMemory();
        System.out.println("===HEAP_SORT===");
        for (Row result: heapResults) {
            System.out.print(result);
        }
        System.out.println();
        System.out.println("===RADIX_SORT===");
        Iterable<Row> radixResults = benchmark.benchmarkRadixSortMemory();
        for (Row result: radixResults) {
            System.out.print(result);
        }
        System.out.println();
        System.out.println("===TIM_SORT===");
        Iterable<Row> timResults = benchmark.benchmarkTimSortMemory();
        for (Row result: timResults) {
            System.out.print(result);
        }
        System.out.println();

        String timestamp = new SimpleDateFormat("yyyyMMdd-HHmmss").format(new Date());
        String resultFile = "target/benchmark-results/memory-" + timestamp + ".json";

        Options opt = new OptionsBuilder()
            .include(OrderMemoryBenchmark.class.getSimpleName())
            .addProfiler(GCProfiler.class)
            .jvmArgs("-Xms2g", "-Xmx4g")
            .result(resultFile)
            .resultFormat(ResultFormatType.JSON)
            .build();
        
        new Runner(opt).run();
    }
}