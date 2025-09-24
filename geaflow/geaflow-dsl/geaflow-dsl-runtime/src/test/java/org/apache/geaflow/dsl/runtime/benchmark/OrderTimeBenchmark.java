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
import org.apache.geaflow.common.binary.BinaryString;
import org.apache.geaflow.common.type.IType;
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
import org.openjdk.jmh.results.format.ResultFormatType;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
@Warmup(iterations = 3, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 2, timeUnit = TimeUnit.SECONDS)
@Fork(2)
public class OrderTimeBenchmark {
    private static final String CHARS = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
    
    @Param({"1000"})
    private int dataSize;
    
    @Param({"1000"})
    private int topN;
    
    @Param({"RANDOM", "SORTED", "REVERSE_SORTED", "PARTIAL_SORTED", "DUPLICATED"})
    private String dataPattern;
    
    @Param({"STRING"})
    private String dataType;
    
    private OrderByFunction orderByFunction;
    private List<Row> testData;
    private SortInfo sortInfo = new SortInfo();
    
    @Setup(Level.Trial)
    public void setupBenchmark() {
        // Create sort expression
        setupOrderByExpressions();
        
        // Generate test data
        testData = generateTestData();
    }
    
    private void setupOrderByExpressions() {
        IType<?> fieldType;
        switch (dataType) {
            case "INTEGER":
                fieldType = Types.INTEGER;
                break;
            case "DOUBLE":
                fieldType = Types.DOUBLE;
                break;
            case "STRING":
                fieldType = Types.BINARY_STRING;
                break;
            default:
                fieldType = Types.INTEGER;
        }
        
        List<OrderByField> orderByFields = new ArrayList<>(2);

        // Primary sort field
        Expression expression1 = new FieldExpression(0, fieldType);
        OrderByField orderByField1 = new OrderByField();
        orderByField1.expression = expression1;
        orderByField1.order = ORDER.ASC;
        orderByFields.add(orderByField1);

        // Add a secondary sort field (for testing multi-field sorting performance)
        Expression expression2 = new FieldExpression(1, Types.INTEGER);
        OrderByField orderByField2 = new OrderByField();
        orderByField2.expression = expression2;
        orderByField2.order = ORDER.ASC;
        orderByFields.add(orderByField2);

        sortInfo.orderByFields = orderByFields;
        sortInfo.fetch = topN;
    }
    
    private List<Row> generateTestData() {
        List<Row> data = new ArrayList<>(dataSize);
        Random random = new Random(42); // Fixed seeds ensure reproducibility
        
        for (int i = 0; i < dataSize; i++) {
            Object[] values = new Object[2];
            
            // Generate the value of the primary sort field
            switch (dataType) {
                case "INTEGER":
                    values[0] = generateIntegerValue(i, random);
                    break;
                case "DOUBLE":
                    values[0] = generateDoubleValue(i, random);
                    break;
                case "STRING":
                    values[0] = BinaryString.fromString(generateStringValue(i, random));
                    break;
                default:
                    return data;
            }
            
            // Generate the value of the secondary sort field
            values[1] = random.nextInt(100);
            
            data.add(ObjectRow.create(values));
        }
        
        return data;
    }
    
    private Integer generateIntegerValue(int index, Random random) {
        switch (dataPattern) {
            case "RANDOM":
                return random.nextInt(dataSize * 10);
            case "SORTED":
                return index;
            case "REVERSE_SORTED":
                return dataSize - index;
            case "PARTIAL_SORTED":
                // 70% ordered, 30% random
                return index < dataSize * 0.7 ? index : random.nextInt(dataSize);
            case "DUPLICATED":
                // Generate a large number of repeated values
                return random.nextInt(dataSize / 10);
            default:
                return random.nextInt(dataSize);
        }
    }
    
    private Double generateDoubleValue(int index, Random random) {
        switch (dataPattern) {
            case "RANDOM":
                return random.nextDouble() * dataSize * 10;
            case "SORTED":
                return (double) index + random.nextDouble();
            case "REVERSE_SORTED":
                return (double) (dataSize - index) + random.nextDouble();
            case "PARTIAL_SORTED":
                return index < dataSize * 0.7
                ? (double) index + random.nextDouble()
                : random.nextDouble() * dataSize;
            case "DUPLICATED":
                return (double) (random.nextInt(dataSize / 10)) + random.nextDouble();
            default:
                return random.nextDouble() * dataSize;
        }
    }
    
    private String generateStringValue(int index, Random random) {
        String[] prefixes = {"A", "B", "C", "D", "E", "F", "G", "H", "I", "J"};
        
        switch (dataPattern) {
            case "RANDOM":
                return generateRandomString(1, 101, random);
            case "SORTED":
                return String.format("R%0100d", index);
            case "REVERSE_SORTED":
                return String.format("R%0100d", dataSize - index);
            case "PARTIAL_SORTED":
                return index < dataSize * 0.7
                    ? String.format("R%0100d", index)
                    : generateRandomString(1, 101, random);
            case "DUPLICATED":
                return prefixes[random.nextInt(3)]
                    + String.format("%0100d", random.nextInt(dataSize / 10));
            default:
                return String.format("R%0100d", random.nextInt(dataSize));
        }
    }

    private String generateRandomString(int length, Random random) {
        StringBuilder sb = new StringBuilder(length);
        for (int i = 0; i < length; i++) {
            sb.append(CHARS.charAt(random.nextInt(CHARS.length())));
        }
        return sb.toString();
    }

    private String generateRandomString(int minLength, int maxLength, Random random) {
        int length = minLength + random.nextInt(maxLength - minLength + 1);
        return generateRandomString(length, random);
    }
    
    
    @Benchmark
    public Iterable<Row> benchmarkHeapSort() {
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
    public Iterable<Row> benchmarkRadixSort() {
        List<Row> inputData = new ArrayList<>(testData);
        
        orderByFunction = new OrderByRadixSort(sortInfo);
        orderByFunction.open(null);

        for (int i = 0; i < dataSize; i++) {
            orderByFunction.process(inputData.get(i));
        }

        return orderByFunction.finish();
    }
    
    @Benchmark
    public Iterable<Row> benchmarkTimSort() {
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
        OrderTimeBenchmark benchmark = new OrderTimeBenchmark();
        benchmark.dataSize = 10;
        benchmark.topN = 10;
        benchmark.dataPattern = "RANDOM";
        benchmark.dataType = "INTEGER";
        benchmark.setupBenchmark();
        Iterable<Row> heapResults = benchmark.benchmarkHeapSort();
        System.out.println("===HEAP_SORT===");
        for (Row result: heapResults) {
            System.out.print(result);
        }
        System.out.println();
        System.out.println("===RADIX_SORT===");
        Iterable<Row> radixResults = benchmark.benchmarkRadixSort();
        for (Row result: radixResults) {
            System.out.print(result);
        }
        System.out.println();
        System.out.println("===TIM_SORT===");
        Iterable<Row> timResults = benchmark.benchmarkTimSort();
        for (Row result: timResults) {
            System.out.print(result);
        }
        System.out.println();

        String timestamp = new SimpleDateFormat("yyyyMMdd-HHmmss").format(new Date());
        String resultFile = "target/benchmark-results/time-" + timestamp + ".json";

        Options opt = new OptionsBuilder()
            .include(OrderTimeBenchmark.class.getSimpleName())
            .jvmArgs("-Xms4g", "-Xmx8g", "-XX:+UseG1GC")
            .result(resultFile)
            .resultFormat(ResultFormatType.JSON)
            .build();
        
        new Runner(opt).run();
    }
}