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
import org.apache.geaflow.dsl.connector.api.window.SizeFetchWindow;
import org.bson.Document;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.utility.DockerImageName;
import org.testng.Assert;
import org.testng.SkipException;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class MongoTableConnectorIT {

    private static final String DATABASE = "geaflow";
    private static final String COLLECTION = "records";
    private static final TableSchema SCHEMA = new TableSchema(
        new TableField("id", Types.INTEGER, false),
        new TableField("name", Types.BINARY_STRING),
        new TableField("active", Types.BOOLEAN));

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

        Assert.assertEquals(ids, new HashSet<>(java.util.Arrays.asList(1, 2, 3)));
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

    private Row row(int id, String name) {
        return ObjectRow.create(id, BinaryString.fromString(name), true);
    }

    private long countDocuments() {
        try (MongoClient client = MongoClients.create(container.getReplicaSetUrl())) {
            return client.getDatabase(DATABASE).getCollection(COLLECTION).countDocuments();
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
