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

package org.apache.geaflow.dsl.connector.mongodb;

import com.mongodb.MongoException;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.Sorts;
import java.io.IOException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import org.apache.geaflow.api.context.RuntimeContext;
import org.apache.geaflow.api.window.WindowType;
import org.apache.geaflow.common.config.ConfigKey;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.dsl.common.exception.GeaFlowDSLException;
import org.apache.geaflow.dsl.common.types.TableSchema;
import org.apache.geaflow.dsl.connector.api.FetchData;
import org.apache.geaflow.dsl.connector.api.Offset;
import org.apache.geaflow.dsl.connector.api.Partition;
import org.apache.geaflow.dsl.connector.api.TableSource;
import org.apache.geaflow.dsl.connector.api.serde.TableDeserializer;
import org.apache.geaflow.dsl.connector.api.window.FetchWindow;
import org.bson.Document;

public class MongoTableSource implements TableSource {

    private String uri;
    private String databaseName;
    private String collectionName;
    private int partitionNum;
    private String partitionField;
    private long lowerBound;
    private long upperBound;
    private TableSchema schema;

    private transient MongoClient client;
    private transient MongoCollection<Document> collection;

    @Override
    public void init(Configuration tableConf, TableSchema tableSchema) {
        this.uri = required(tableConf, MongoConfigKeys.GEAFLOW_DSL_MONGODB_URI);
        this.databaseName = required(tableConf, MongoConfigKeys.GEAFLOW_DSL_MONGODB_DATABASE);
        this.collectionName = required(tableConf, MongoConfigKeys.GEAFLOW_DSL_MONGODB_COLLECTION);
        this.partitionNum = tableConf.getInteger(MongoConfigKeys.GEAFLOW_DSL_MONGODB_PARTITION_NUM);
        this.schema = tableSchema;

        if (partitionNum <= 0) {
            throw new GeaFlowDSLException("MongoDB partition number must be greater than zero");
        }
        if (partitionNum > 1) {
            this.partitionField = required(tableConf,
                MongoConfigKeys.GEAFLOW_DSL_MONGODB_PARTITION_FIELD);
            requireConfig(tableConf, MongoConfigKeys.GEAFLOW_DSL_MONGODB_PARTITION_LOWERBOUND);
            requireConfig(tableConf, MongoConfigKeys.GEAFLOW_DSL_MONGODB_PARTITION_UPPERBOUND);
            this.lowerBound = tableConf.getLong(
                MongoConfigKeys.GEAFLOW_DSL_MONGODB_PARTITION_LOWERBOUND);
            this.upperBound = tableConf.getLong(
                MongoConfigKeys.GEAFLOW_DSL_MONGODB_PARTITION_UPPERBOUND);
            if (lowerBound >= upperBound) {
                throw new GeaFlowDSLException(
                    "MongoDB partition upper bound must be greater than lower bound");
            }
        }
    }

    @Override
    public void open(RuntimeContext context) {
        try {
            client = MongoClients.create(uri);
            collection = client.getDatabase(databaseName).getCollection(collectionName);
        } catch (MongoException | IllegalArgumentException e) {
            close();
            throw new GeaFlowDSLException("Failed to create MongoDB source client", e);
        }
    }

    @Override
    public List<Partition> listPartitions() {
        if (partitionNum == 1) {
            return Collections.singletonList(new MongoPartition(collectionName, 0));
        }

        BigInteger lower = BigInteger.valueOf(lowerBound);
        BigInteger range = BigInteger.valueOf(upperBound).subtract(lower);
        int count = range.min(BigInteger.valueOf(partitionNum)).intValueExact();
        BigInteger[] strideAndRemainder = range.divideAndRemainder(BigInteger.valueOf(count));
        BigInteger current = lower;
        List<Partition> partitions = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            BigInteger increment = strideAndRemainder[0];
            if (i < strideAndRemainder[1].intValue()) {
                increment = increment.add(BigInteger.ONE);
            }
            BigInteger next = current.add(increment);
            partitions.add(new MongoPartition(collectionName, i, partitionField,
                current.longValueExact(), next.longValueExact()));
            current = next;
        }
        return partitions;
    }

    @Override
    public List<Partition> listPartitions(int parallelism) {
        return listPartitions();
    }

    @Override
    @SuppressWarnings("unchecked")
    public <IN> TableDeserializer<IN> getDeserializer(Configuration conf) {
        return (TableDeserializer<IN>) new MongoRowConverter(schema);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> FetchData<T> fetch(Partition partition, Optional<Offset> startOffset,
                                  FetchWindow windowInfo) throws IOException {
        if (collection == null) {
            throw new GeaFlowDSLException("MongoDB source is not open");
        }
        if (!(partition instanceof MongoPartition)) {
            throw new GeaFlowDSLException("Invalid MongoDB partition");
        }
        MongoPartition mongoPartition = (MongoPartition) partition;
        if (!collectionName.equals(mongoPartition.getCollection())) {
            throw new GeaFlowDSLException("MongoDB partition belongs to another collection");
        }
        if (windowInfo.getType() != WindowType.SIZE_TUMBLING_WINDOW
            && windowInfo.getType() != WindowType.ALL_WINDOW) {
            throw new GeaFlowDSLException("Unsupported MongoDB fetch window: {}",
                windowInfo.getType());
        }

        long offset = startOffset.map(Offset::getOffset).orElse(0L);
        if (offset < 0 || offset > Integer.MAX_VALUE) {
            throw new GeaFlowDSLException("MongoDB offset is out of range: {}", offset);
        }

        boolean allWindow = windowInfo.getType() == WindowType.ALL_WINDOW;
        int limit = 0;
        if (!allWindow) {
            long windowSize = windowInfo.windowSize();
            if (windowSize <= 0 || windowSize > Integer.MAX_VALUE) {
                throw new GeaFlowDSLException("MongoDB fetch window size is out of range: {}",
                    windowSize);
            }
            limit = (int) windowSize;
        }

        List<Document> documents = new ArrayList<>();
        try {
            FindIterable<Document> query = collection.find(mongoPartition.toFilter())
                .sort(mongoPartition.hasRange()
                    ? Sorts.ascending(mongoPartition.getField(), "_id") : Sorts.ascending("_id"))
                .skip((int) offset);
            if (!allWindow) {
                query = query.limit(limit);
            }
            try (MongoCursor<Document> cursor = query.iterator()) {
                while (cursor.hasNext()) {
                    documents.add(cursor.next());
                }
            }
        } catch (MongoException e) {
            throw new IOException("Failed to read MongoDB collection " + collectionName, e);
        }

        MongoOffset nextOffset = new MongoOffset(offset + documents.size());
        boolean finished = allWindow || documents.size() < limit;
        return (FetchData<T>) FetchData.createStreamFetch(documents, nextOffset, finished);
    }

    @Override
    public void close() {
        if (client != null) {
            client.close();
            client = null;
            collection = null;
        }
    }

    private static String required(Configuration conf, ConfigKey key) {
        requireConfig(conf, key);
        String value = conf.getString(key);
        if (value == null || value.trim().isEmpty()) {
            throw new GeaFlowDSLException("MongoDB configuration '{}' must not be blank",
                key.getKey());
        }
        return value;
    }

    private static void requireConfig(Configuration conf, ConfigKey key) {
        if (!conf.contains(key)) {
            throw new GeaFlowDSLException("Missing MongoDB configuration '{}'", key.getKey());
        }
    }
}
