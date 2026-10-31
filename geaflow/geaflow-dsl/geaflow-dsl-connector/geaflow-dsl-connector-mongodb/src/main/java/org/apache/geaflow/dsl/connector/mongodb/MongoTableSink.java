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

import com.mongodb.MongoBulkWriteException;
import com.mongodb.MongoException;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.InsertManyOptions;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.geaflow.api.context.RuntimeContext;
import org.apache.geaflow.common.config.ConfigKey;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.dsl.common.data.Row;
import org.apache.geaflow.dsl.common.exception.GeaFlowDSLException;
import org.apache.geaflow.dsl.common.types.StructType;
import org.apache.geaflow.dsl.connector.api.TableSink;
import org.bson.Document;

public class MongoTableSink implements TableSink {

    private String uri;
    private String databaseName;
    private String collectionName;
    private int batchSize;
    private MongoRowConverter converter;
    private List<Document> batch;

    private transient MongoClient client;
    private transient MongoCollection<Document> collection;

    @Override
    public void init(Configuration tableConf, StructType schema) {
        this.uri = required(tableConf, MongoConfigKeys.GEAFLOW_DSL_MONGODB_URI);
        this.databaseName = required(tableConf, MongoConfigKeys.GEAFLOW_DSL_MONGODB_DATABASE);
        this.collectionName = required(tableConf, MongoConfigKeys.GEAFLOW_DSL_MONGODB_COLLECTION);
        this.batchSize = tableConf.getInteger(MongoConfigKeys.GEAFLOW_DSL_MONGODB_BATCH_SIZE);
        if (batchSize <= 0) {
            throw new GeaFlowDSLException("MongoDB batch size must be greater than zero");
        }
        this.converter = new MongoRowConverter(schema);
        this.batch = new ArrayList<>(batchSize);
    }

    @Override
    public void open(RuntimeContext context) {
        try {
            client = MongoClients.create(uri);
            collection = client.getDatabase(databaseName).getCollection(collectionName);
        } catch (MongoException | IllegalArgumentException e) {
            close();
            throw new GeaFlowDSLException("Failed to create MongoDB sink client", e);
        }
    }

    @Override
    public void write(Row row) throws IOException {
        if (collection == null) {
            throw new GeaFlowDSLException("MongoDB sink is not open");
        }
        batch.add(converter.toDocument(row));
        if (batch.size() >= batchSize) {
            flush();
        }
    }

    @Override
    public void finish() throws IOException {
        flush();
    }

    @Override
    public void close() {
        if (client != null) {
            client.close();
            client = null;
            collection = null;
        }
    }

    private void flush() throws IOException {
        if (batch.isEmpty()) {
            return;
        }
        try {
            collection.insertMany(batch, new InsertManyOptions().ordered(true));
            batch.clear();
        } catch (MongoBulkWriteException e) {
            String writeConcernError = e.getWriteConcernError() == null ? ""
                : "; write concern error: " + e.getWriteConcernError();
            throw new IOException("MongoDB bulk write failed for collection " + collectionName
                + "; successful inserts in this batch: "
                + e.getWriteResult().getInsertedCount() + "; write errors: "
                + e.getWriteErrors() + writeConcernError, e);
        } catch (MongoException e) {
            throw new IOException("Failed to write MongoDB collection " + collectionName, e);
        }
    }

    private static String required(Configuration conf, ConfigKey key) {
        if (!conf.contains(key)) {
            throw new GeaFlowDSLException("Missing MongoDB configuration '{}'", key.getKey());
        }
        String value = conf.getString(key);
        if (value == null || value.trim().isEmpty()) {
            throw new GeaFlowDSLException("MongoDB configuration '{}' must not be blank",
                key.getKey());
        }
        return value;
    }
}
