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

package org.apache.geaflow.dsl.connector.random;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Random;
import org.apache.geaflow.api.context.RuntimeContext;
import org.apache.geaflow.api.window.WindowType;
import org.apache.geaflow.common.binary.BinaryString;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.common.type.IType;
import org.apache.geaflow.common.type.Types;
import org.apache.geaflow.common.utils.SleepUtils;
import org.apache.geaflow.dsl.common.data.Row;
import org.apache.geaflow.dsl.common.data.impl.ObjectRow;
import org.apache.geaflow.dsl.common.exception.GeaFlowDSLException;
import org.apache.geaflow.dsl.common.types.TableField;
import org.apache.geaflow.dsl.common.types.TableSchema;
import org.apache.geaflow.dsl.connector.api.FetchData;
import org.apache.geaflow.dsl.connector.api.Offset;
import org.apache.geaflow.dsl.connector.api.Partition;
import org.apache.geaflow.dsl.connector.api.TableSource;
import org.apache.geaflow.dsl.connector.api.serde.TableDeserializer;
import org.apache.geaflow.dsl.connector.api.serde.impl.RowTableDeserializer;
import org.apache.geaflow.dsl.connector.api.window.FetchWindow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RandomTableSource implements TableSource {

    private static final Logger LOGGER = LoggerFactory.getLogger(RandomTableSource.class);

    private TableSchema schema = null;
    private double rate = 1.0;
    private long maxBatch = 100L;

    @Override
    public void init(Configuration tableConf, TableSchema tableSchema) {
        this.schema = tableSchema;
        this.rate = tableConf.getDouble(RandomSourceConfigKeys.GEAFLOW_DSL_RANDOM_SOURCE_RATE);
        this.maxBatch = tableConf.getLong(RandomSourceConfigKeys.GEAFLOW_DSL_RANDOM_SOURCE_MAX_BATCH);
    }

    @Override
    public void open(RuntimeContext context) {
        this.hashCode();
    }

    private static List<Partition> singletonPartition() {
        List<Partition> singletonPartition = new ArrayList<>();
        singletonPartition.add(new RandomPartition());
        return singletonPartition;
    }

    @Override
    public List<Partition> listPartitions() {
        return singletonPartition();
    }

    @Override
    public List<Partition> listPartitions(int parallelism) {
        return singletonPartition();
    }

    @Override
    public <IN> TableDeserializer<IN> getDeserializer(Configuration conf) {
        return (TableDeserializer<IN>) new RowTableDeserializer();
    }

    @Override
    public <T> FetchData<T> fetch(Partition partition, Optional<Offset> startOffset,
                                  FetchWindow windowInfo) throws IOException {
        long windowSize;
        if (windowInfo.getType() == WindowType.ALL_WINDOW) {
            windowSize = (long) rate;
        } else if (windowInfo.getType() == WindowType.SIZE_TUMBLING_WINDOW) {
            //Control wait time based on the rate.
            windowSize = windowInfo.windowSize();
            SleepUtils.sleepMilliSecond((long) (1000 * (windowSize / rate)));
        } else if (windowInfo.getType() == WindowType.FIXED_TIME_TUMBLING_WINDOW) {
            //Control the number of tuples based on the rate.
            if (windowInfo.windowSize() > Integer.MAX_VALUE) {
                throw new GeaFlowDSLException("Random table source window size is overflow:{}",
                    windowInfo.windowSize());
            }
            long seconds = (int) windowInfo.windowSize();
            windowSize = (long) (seconds * rate);
        } else {
            throw new GeaFlowDSLException("File table source not support window:{}", windowInfo.getType());
        }
        List<Row> randomContents = new ArrayList<>();
        Random random = new Random();
        for (long i = 0; i < windowSize; i++) {
            Row row = createRandomRow(schema, random);
            randomContents.add(row);
        }
        long thisBatchId = 1L;
        if (startOffset.isPresent()) {
            thisBatchId = startOffset.get().getOffset();
        }
        if (windowInfo.getType() == WindowType.ALL_WINDOW) {
            return (FetchData<T>) FetchData.createBatchFetch(randomContents.iterator(),
                new RandomSourceOffset(thisBatchId + 1));
        } else {
            boolean isFinish = thisBatchId >= maxBatch;
            return (FetchData<T>) FetchData.createStreamFetch(randomContents,
                new RandomSourceOffset(thisBatchId + 1), isFinish);
        }
    }

    private static Row createRandomRow(TableSchema schema, Random random) {
        List<TableField> fields = schema.getFields();
        Object[] objects = new Object[schema.size()];
        for (int i = 0; i < fields.size(); i++) {
            TableField field = fields.get(i);
            objects[i] = createRandomCol(field, random);
        }
        return ObjectRow.create(objects);
    }

    private static Object createRandomCol(TableField field, Random random) {
        String fieldName = field.getName();
        IType type = field.getType();
        switch (type.getName()) {
            case Types.TYPE_NAME_BOOLEAN:
                return random.nextBoolean();
            case Types.TYPE_NAME_BYTE:
                return (byte) random.nextInt(Byte.MAX_VALUE + 1);
            case Types.TYPE_NAME_SHORT:
                return (short) random.nextInt(Short.MAX_VALUE + 1);
            case Types.TYPE_NAME_INTEGER:
                return random.nextInt(100);
            case Types.TYPE_NAME_LONG:
                return (long) random.nextInt(1000000);
            case Types.TYPE_NAME_FLOAT:
                return random.nextFloat() * 100.0;
            case Types.TYPE_NAME_DOUBLE:
                return random.nextDouble() * 1000000.0;
            case Types.TYPE_NAME_STRING:
                return fieldName + "_" + random.nextInt(1000000);
            case Types.TYPE_NAME_BINARY_STRING:
                return BinaryString.fromString(fieldName + "_" + random.nextInt(1000000));
            default:
                throw new RuntimeException("Cannot create random value for type: " + type);
        }
    }

    @Override
    public void close() {

    }

    public static class RandomPartition implements Partition {

        public RandomPartition() {
        }

        @Override
        public String getName() {
            return this.getClass().getName();
        }

        @Override
        public void setIndex(int index, int parallel) {
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            return o != null && getClass() == o.getClass();
        }

        @Override
        public int hashCode() {
            return Objects.hash(this.getClass().getName());
        }
    }

    public static class RandomSourceOffset implements Offset {

        private final long batchId;

        public RandomSourceOffset(long batchId) {
            this.batchId = batchId;
        }

        @Override
        public String humanReadable() {
            return "batch:" + batchId;
        }

        @Override
        public long getOffset() {
            return batchId;
        }

        @Override
        public boolean isTimestamp() {
            return false;
        }
    }
}
