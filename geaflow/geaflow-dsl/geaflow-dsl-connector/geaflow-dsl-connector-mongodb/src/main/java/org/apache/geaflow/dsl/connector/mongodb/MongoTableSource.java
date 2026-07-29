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
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Sorts;
import java.io.IOException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
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
import org.bson.BsonDocument;
import org.bson.BsonValue;
import org.bson.Document;
import org.bson.conversions.Bson;

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

        MongoOffset offset = getMongoOffset(startOffset);

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

        Bson filter = createFilter(mongoPartition, offset);
        if (allWindow) {
            try {
                MongoCursor<Document> cursor = collection.find(filter).iterator();
                return (FetchData<T>) FetchData.createBatchFetch(
                    new MongoCursorIterator(cursor, collectionName), offset);
            } catch (MongoException e) {
                throw new IOException("Failed to read MongoDB collection " + collectionName, e);
            }
        }

        List<Document> documents = new ArrayList<>();
        try {
            FindIterable<Document> query = collection.find(filter)
                .sort(mongoPartition.hasRange()
                    ? Sorts.ascending(mongoPartition.getField(), "_id") : Sorts.ascending("_id"))
                .limit(limit);
            try (MongoCursor<Document> cursor = query.iterator()) {
                while (cursor.hasNext()) {
                    documents.add(cursor.next());
                }
            }
        } catch (MongoException e) {
            throw new IOException("Failed to read MongoDB collection " + collectionName, e);
        }

        MongoOffset nextOffset = createNextOffset(offset, mongoPartition, documents);
        boolean finished = documents.size() < limit;
        return (FetchData<T>) FetchData.createStreamFetch(documents, nextOffset, finished);
    }

    static Bson createFilter(MongoPartition partition, MongoOffset offset) {
        if (!offset.hasBookmark()) {
            return partition.toFilter();
        }

        Bson bookmarkFilter = createBookmarkFilter(partition, offset.getBookmark());
        return partition.hasRange()
            ? Filters.and(partition.toFilter(), bookmarkFilter) : bookmarkFilter;
    }

    static Bson createBookmarkFilter(MongoPartition partition, BsonDocument bookmark) {
        BsonValue lastId = requiredBookmarkValue(bookmark, MongoOffset.ID_FIELD);
        if (!partition.hasRange()) {
            return Filters.gt(MongoOffset.ID_FIELD, lastId);
        }

        BsonValue lastPartitionValue = requiredBookmarkValue(bookmark,
            MongoOffset.PARTITION_VALUE_FIELD);
        return Filters.or(
            Filters.gt(partition.getField(), lastPartitionValue),
            Filters.and(
                Filters.eq(partition.getField(), lastPartitionValue),
                Filters.gt(MongoOffset.ID_FIELD, lastId)));
    }

    private static BsonValue requiredBookmarkValue(BsonDocument bookmark, String field) {
        BsonValue value = bookmark.get(field);
        if (value == null) {
            throw new GeaFlowDSLException("MongoDB offset is missing bookmark field '{}'",
                field);
        }
        return value;
    }

    private MongoOffset createNextOffset(MongoOffset offset, MongoPartition partition,
                                         List<Document> documents) {
        if (documents.isEmpty()) {
            return offset;
        }

        Document lastDocument = documents.get(documents.size() - 1);
        BsonDocument bsonDocument = lastDocument.toBsonDocument(Document.class,
            collection.getCodecRegistry());
        BsonValue lastId = getDocumentValue(bsonDocument, MongoOffset.ID_FIELD);
        if (lastId == null) {
            throw new GeaFlowDSLException("MongoDB document is missing field '{}'",
                MongoOffset.ID_FIELD);
        }

        BsonDocument bookmark = new BsonDocument(MongoOffset.ID_FIELD, lastId);
        if (partition.hasRange()) {
            BsonValue partitionValue = getDocumentValue(bsonDocument, partition.getField());
            if (partitionValue == null) {
                throw new GeaFlowDSLException("MongoDB document is missing partition field '{}'",
                    partition.getField());
            }
            bookmark.append(MongoOffset.PARTITION_VALUE_FIELD, partitionValue);
        }

        long nextOffset;
        try {
            nextOffset = Math.addExact(offset.getOffset(), documents.size());
        } catch (ArithmeticException e) {
            throw new GeaFlowDSLException("MongoDB offset exceeds the supported range", e);
        }
        return new MongoOffset(nextOffset, bookmark);
    }

    private static BsonValue getDocumentValue(BsonDocument document, String field) {
        BsonValue value = document;
        for (String name : field.split("\\.")) {
            if (!value.isDocument()) {
                return null;
            }
            value = value.asDocument().get(name);
            if (value == null) {
                return null;
            }
        }
        return value;
    }

    private static MongoOffset getMongoOffset(Optional<Offset> startOffset) {
        if (!startOffset.isPresent()) {
            return new MongoOffset(0L);
        }
        Offset offset = startOffset.get();
        if (!(offset instanceof MongoOffset)) {
            throw new GeaFlowDSLException("Invalid MongoDB offset");
        }
        MongoOffset mongoOffset = (MongoOffset) offset;
        if (mongoOffset.getOffset() > 0 && !mongoOffset.hasBookmark()) {
            throw new GeaFlowDSLException("MongoDB offset is missing a bookmark");
        }
        return mongoOffset;
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

    private static class MongoCursorIterator implements Iterator<Document> {

        private final MongoCursor<Document> cursor;
        private final String collectionName;
        private boolean closed;

        private MongoCursorIterator(MongoCursor<Document> cursor, String collectionName) {
            this.cursor = cursor;
            this.collectionName = collectionName;
        }

        @Override
        public boolean hasNext() {
            if (closed) {
                return false;
            }
            try {
                boolean hasNext = cursor.hasNext();
                if (!hasNext) {
                    close();
                }
                return hasNext;
            } catch (MongoException e) {
                close();
                throw readException(e);
            }
        }

        @Override
        public Document next() {
            try {
                return cursor.next();
            } catch (MongoException e) {
                close();
                throw readException(e);
            }
        }

        private void close() {
            if (!closed) {
                closed = true;
                cursor.close();
            }
        }

        private GeaFlowDSLException readException(MongoException cause) {
            return new GeaFlowDSLException(
                "Failed to read MongoDB collection " + collectionName, cause);
        }
    }
}
