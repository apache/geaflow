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

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import org.apache.geaflow.common.binary.BinaryString;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.common.type.Types;
import org.apache.geaflow.dsl.common.data.Row;
import org.apache.geaflow.dsl.common.data.impl.ObjectRow;
import org.apache.geaflow.dsl.common.types.TableField;
import org.apache.geaflow.dsl.common.types.TableSchema;
import org.apache.geaflow.dsl.connector.api.FetchData;
import org.apache.geaflow.dsl.connector.api.Offset;
import org.apache.geaflow.dsl.connector.api.Partition;
import org.apache.geaflow.dsl.connector.api.serde.TableDeserializer;
import org.apache.geaflow.dsl.connector.api.window.AllFetchWindow;
import org.apache.geaflow.dsl.connector.api.window.SizeFetchWindow;
import org.bson.Document;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.utility.DockerImageName;
import org.testng.Assert;
import org.testng.SkipException;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class MongoTableConnectorIT {

    private static final String DATABASE = "geaflow";
    private static final String COLLECTION = "records";
    private static final TableSchema SCHEMA = new TableSchema(
        new TableField("id", Types.INTEGER, false),
        new TableField("name", Types.BINARY_STRING),
        new TableField("active", Types.BOOLEAN));
    private static final TableSchema BOOKMARK_SCHEMA = new TableSchema(
        new TableField("_id", Types.BINARY_STRING, false),
        new TableField("group_id", Types.INTEGER, false));

    private MongoDBContainer container;

    @BeforeClass
    public void startMongoDB() {
        boolean dockerAvailable;
        try {
            dockerAvailable = DockerClientFactory.instance().isDockerAvailable();
        } catch (RuntimeException e) {
            throw new SkipException("Docker is not available", e);
        }
        if (!dockerAvailable) {
            throw new SkipException("Docker is not available");
        }
        container = new MongoDBContainer(DockerImageName.parse("mongo:6.0.14"));
        container.start();
    }

    @AfterClass(alwaysRun = true)
    public void stopMongoDB() {
        if (container != null) {
            container.stop();
        }
    }

    @BeforeMethod
    public void clearCollection() {
        try (MongoClient client = MongoClients.create(container.getReplicaSetUrl())) {
            client.getDatabase(DATABASE).getCollection(COLLECTION).deleteMany(new Document());
        }
    }

    @Test
    public void testBatchWriteAndPartitionedRead() throws IOException {
        Configuration sinkConf = baseConfig();
        sinkConf.put(MongoConfigKeys.GEAFLOW_DSL_MONGODB_BATCH_SIZE, "2");
        MongoTableSink sink = new MongoTableSink();
        sink.init(sinkConf, SCHEMA);
        sink.open(null);

        sink.write(row(1, "alice"));
        sink.write(row(2, "bob"));
        Assert.assertEquals(countDocuments(), 2L);
        sink.write(row(3, "carol"));
        Assert.assertEquals(countDocuments(), 2L);
        sink.finish();
        sink.close();
        Assert.assertEquals(countDocuments(), 3L);

        Configuration sourceConf = baseConfig();
        sourceConf.put(MongoConfigKeys.GEAFLOW_DSL_MONGODB_PARTITION_NUM, "2");
        sourceConf.put(MongoConfigKeys.GEAFLOW_DSL_MONGODB_PARTITION_FIELD, "id");
        sourceConf.put(MongoConfigKeys.GEAFLOW_DSL_MONGODB_PARTITION_LOWERBOUND, "1");
        sourceConf.put(MongoConfigKeys.GEAFLOW_DSL_MONGODB_PARTITION_UPPERBOUND, "4");
        MongoTableSource source = new MongoTableSource();
        source.init(sourceConf, SCHEMA);
        source.open(null);

        Set<Integer> ids = new HashSet<>();
        List<Partition> partitions = source.listPartitions();
        for (Partition partition : partitions) {
            readPartition(source, sourceConf, partition, ids);
        }
        source.close();

        Assert.assertEquals(ids, new HashSet<>(Arrays.asList(1, 2, 3)));
    }

    @Test
    public void testCompositeBookmarkPagination() throws IOException {
        insertDocuments(Arrays.asList(
            new Document("_id", "z").append("group_id", 1),
            new Document("_id", "a").append("group_id", 2),
            new Document("_id", "y").append("group_id", 6),
            new Document("_id", "b").append("group_id", 7)));

        Configuration conf = baseConfig();
        conf.put(MongoConfigKeys.GEAFLOW_DSL_MONGODB_PARTITION_NUM, "2");
        conf.put(MongoConfigKeys.GEAFLOW_DSL_MONGODB_PARTITION_FIELD, "group_id");
        conf.put(MongoConfigKeys.GEAFLOW_DSL_MONGODB_PARTITION_LOWERBOUND, "0");
        conf.put(MongoConfigKeys.GEAFLOW_DSL_MONGODB_PARTITION_UPPERBOUND, "10");
        MongoTableSource source = new MongoTableSource();
        source.init(conf, BOOKMARK_SCHEMA);
        source.open(null);

        Set<String> ids = new HashSet<>();
        try {
            for (Partition partition : source.listPartitions()) {
                readStringPartition(source, conf, partition, ids);
            }
        } finally {
            source.close();
        }

        Assert.assertEquals(ids, new HashSet<>(Arrays.asList("z", "a", "y", "b")));
    }

    @Test
    public void testAllWindowRead() throws IOException {
        insertDocuments(Arrays.asList(
            new Document("id", 1),
            new Document("id", 2),
            new Document("id", 3)));

        Configuration conf = baseConfig();
        MongoTableSource source = new MongoTableSource();
        source.init(conf, SCHEMA);
        source.open(null);

        try {
            Partition partition = source.listPartitions().get(0);
            FetchData<Document> fetchData = source.fetch(partition, Optional.empty(),
                new AllFetchWindow(0));
            Assert.assertEquals(fetchData.getDataSize(), -1);
            Assert.assertTrue(fetchData.isFinish());

            int count = 0;
            Iterator<Document> iterator = fetchData.getDataIterator();
            while (iterator.hasNext()) {
                iterator.next();
                count++;
            }
            Assert.assertEquals(count, 3);
        } finally {
            source.close();
        }
    }

    @Test
    public void testOrderedBulkWriteFailure() throws IOException {
        insertDocuments(Arrays.asList(
            new Document("_id", "duplicate").append("group_id", 0)));

        Configuration conf = baseConfig();
        conf.put(MongoConfigKeys.GEAFLOW_DSL_MONGODB_BATCH_SIZE, "3");
        MongoTableSink sink = new MongoTableSink();
        sink.init(conf, BOOKMARK_SCHEMA);
        sink.open(null);

        IOException failure = null;
        try {
            sink.write(bookmarkRow("first", 1));
            sink.write(bookmarkRow("duplicate", 2));
            try {
                sink.write(bookmarkRow("last", 3));
            } catch (IOException e) {
                failure = e;
            }
        } finally {
            sink.close();
        }

        Assert.assertNotNull(failure);
        Assert.assertTrue(failure.getMessage().contains(
            "successful inserts in this batch: 1"));
        Assert.assertTrue(failure.getMessage().contains("write errors:"));
        Assert.assertEquals(countDocuments(), 2L);
        Assert.assertEquals(countDocuments(new Document("_id", "last")), 0L);
    }

    private void readPartition(MongoTableSource source, Configuration conf, Partition partition,
                               Set<Integer> ids) throws IOException {
        Optional<Offset> offset = Optional.empty();
        boolean finished;
        TableDeserializer<Document> deserializer = source.getDeserializer(conf);
        deserializer.init(conf, SCHEMA);
        do {
            FetchData<Document> fetchData = source.fetch(partition, offset,
                new SizeFetchWindow(0, 1));
            Iterator<Document> iterator = fetchData.getDataIterator();
            while (iterator.hasNext()) {
                Row row = deserializer.deserialize(iterator.next()).get(0);
                Integer id = (Integer) row.getField(0, Types.INTEGER);
                Assert.assertTrue(ids.add(id), "Duplicate id: " + id);
            }
            offset = Optional.of(fetchData.getNextOffset());
            finished = fetchData.isFinish();
        } while (!finished);
    }

    private void readStringPartition(MongoTableSource source, Configuration conf,
                                     Partition partition, Set<String> ids) throws IOException {
        Optional<Offset> offset = Optional.empty();
        boolean finished;
        TableDeserializer<Document> deserializer = source.getDeserializer(conf);
        deserializer.init(conf, BOOKMARK_SCHEMA);
        do {
            FetchData<Document> fetchData = source.fetch(partition, offset,
                new SizeFetchWindow(0, 1));
            Iterator<Document> iterator = fetchData.getDataIterator();
            while (iterator.hasNext()) {
                Row row = deserializer.deserialize(iterator.next()).get(0);
                BinaryString id = (BinaryString) row.getField(0, Types.BINARY_STRING);
                Assert.assertTrue(ids.add(id.toString()), "Duplicate id: " + id);
            }
            offset = Optional.of(fetchData.getNextOffset());
            finished = fetchData.isFinish();
        } while (!finished);
    }

    private Row row(int id, String name) {
        return ObjectRow.create(id, BinaryString.fromString(name), true);
    }

    private Row bookmarkRow(String id, int groupId) {
        return ObjectRow.create(BinaryString.fromString(id), groupId);
    }

    private void insertDocuments(List<Document> documents) {
        try (MongoClient client = MongoClients.create(container.getReplicaSetUrl())) {
            client.getDatabase(DATABASE).getCollection(COLLECTION).insertMany(documents);
        }
    }

    private long countDocuments() {
        return countDocuments(new Document());
    }

    private long countDocuments(Document filter) {
        try (MongoClient client = MongoClients.create(container.getReplicaSetUrl())) {
            return client.getDatabase(DATABASE).getCollection(COLLECTION)
                .countDocuments(filter);
        }
    }

    private Configuration baseConfig() {
        Configuration conf = new Configuration();
        conf.put(MongoConfigKeys.GEAFLOW_DSL_MONGODB_URI, container.getReplicaSetUrl());
        conf.put(MongoConfigKeys.GEAFLOW_DSL_MONGODB_DATABASE, DATABASE);
        conf.put(MongoConfigKeys.GEAFLOW_DSL_MONGODB_COLLECTION, COLLECTION);
        return conf;
    }
}
