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

import java.util.List;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.common.type.Types;
import org.apache.geaflow.dsl.common.exception.GeaFlowDSLException;
import org.apache.geaflow.dsl.common.types.TableField;
import org.apache.geaflow.dsl.common.types.TableSchema;
import org.apache.geaflow.dsl.connector.api.Partition;
import org.apache.geaflow.dsl.connector.api.TableConnector;
import org.apache.geaflow.dsl.connector.api.util.ConnectorFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

public class MongoTableConnectorTest {

    private static final TableSchema SCHEMA = new TableSchema(
        new TableField("id", Types.INTEGER, false));

    @Test
    public void testLoadConnector() {
        TableConnector connector = ConnectorFactory.loadConnector("mongodb");
        Assert.assertEquals(connector.getType(), MongoTableConnector.TYPE);
    }

    @Test
    public void testSinglePartition() {
        MongoTableSource source = new MongoTableSource();
        source.init(baseConfig(), SCHEMA);

        List<Partition> partitions = source.listPartitions();

        Assert.assertEquals(partitions.size(), 1);
        MongoPartition partition = (MongoPartition) partitions.get(0);
        Assert.assertFalse(partition.hasRange());
    }

    @Test
    public void testRangePartitions() {
        Configuration conf = baseConfig();
        conf.put(MongoConfigKeys.GEAFLOW_DSL_MONGODB_PARTITION_NUM, "3");
        conf.put(MongoConfigKeys.GEAFLOW_DSL_MONGODB_PARTITION_FIELD, "id");
        conf.put(MongoConfigKeys.GEAFLOW_DSL_MONGODB_PARTITION_LOWERBOUND, "0");
        conf.put(MongoConfigKeys.GEAFLOW_DSL_MONGODB_PARTITION_UPPERBOUND, "10");
        MongoTableSource source = new MongoTableSource();
        source.init(conf, SCHEMA);

        List<Partition> partitions = source.listPartitions();

        Assert.assertEquals(partitions.size(), 3);
        assertBounds((MongoPartition) partitions.get(0), 0L, 4L);
        assertBounds((MongoPartition) partitions.get(1), 4L, 7L);
        assertBounds((MongoPartition) partitions.get(2), 7L, 10L);
    }

    @Test(expectedExceptions = GeaFlowDSLException.class,
        expectedExceptionsMessageRegExp = ".*partition.upperbound.*")
    public void testMissingPartitionBound() {
        Configuration conf = baseConfig();
        conf.put(MongoConfigKeys.GEAFLOW_DSL_MONGODB_PARTITION_NUM, "2");
        conf.put(MongoConfigKeys.GEAFLOW_DSL_MONGODB_PARTITION_FIELD, "id");
        conf.put(MongoConfigKeys.GEAFLOW_DSL_MONGODB_PARTITION_LOWERBOUND, "0");

        new MongoTableSource().init(conf, SCHEMA);
    }

    @Test(expectedExceptions = GeaFlowDSLException.class,
        expectedExceptionsMessageRegExp = ".*batch size.*")
    public void testInvalidBatchSize() {
        Configuration conf = baseConfig();
        conf.put(MongoConfigKeys.GEAFLOW_DSL_MONGODB_BATCH_SIZE, "0");

        new MongoTableSink().init(conf, SCHEMA);
    }

    @Test(expectedExceptions = GeaFlowDSLException.class,
        expectedExceptionsMessageRegExp = ".*partition number.*")
    public void testInvalidPartitionNumber() {
        Configuration conf = baseConfig();
        conf.put(MongoConfigKeys.GEAFLOW_DSL_MONGODB_PARTITION_NUM, "0");

        new MongoTableSource().init(conf, SCHEMA);
    }

    @Test(expectedExceptions = GeaFlowDSLException.class,
        expectedExceptionsMessageRegExp = ".*upper bound.*")
    public void testInvalidPartitionRange() {
        Configuration conf = baseConfig();
        conf.put(MongoConfigKeys.GEAFLOW_DSL_MONGODB_PARTITION_NUM, "2");
        conf.put(MongoConfigKeys.GEAFLOW_DSL_MONGODB_PARTITION_FIELD, "id");
        conf.put(MongoConfigKeys.GEAFLOW_DSL_MONGODB_PARTITION_LOWERBOUND, "10");
        conf.put(MongoConfigKeys.GEAFLOW_DSL_MONGODB_PARTITION_UPPERBOUND, "10");

        new MongoTableSource().init(conf, SCHEMA);
    }

    @Test(expectedExceptions = GeaFlowDSLException.class,
        expectedExceptionsMessageRegExp = ".*mongodb.uri.*")
    public void testMissingUri() {
        Configuration conf = new Configuration();
        conf.put(MongoConfigKeys.GEAFLOW_DSL_MONGODB_DATABASE, "geaflow");
        conf.put(MongoConfigKeys.GEAFLOW_DSL_MONGODB_COLLECTION, "records");

        new MongoTableSource().init(conf, SCHEMA);
    }

    @Test
    public void testPartitionCountLimitedByRange() {
        Configuration conf = baseConfig();
        conf.put(MongoConfigKeys.GEAFLOW_DSL_MONGODB_PARTITION_NUM, "4");
        conf.put(MongoConfigKeys.GEAFLOW_DSL_MONGODB_PARTITION_FIELD, "id");
        conf.put(MongoConfigKeys.GEAFLOW_DSL_MONGODB_PARTITION_LOWERBOUND, "0");
        conf.put(MongoConfigKeys.GEAFLOW_DSL_MONGODB_PARTITION_UPPERBOUND, "2");
        MongoTableSource source = new MongoTableSource();
        source.init(conf, SCHEMA);

        List<Partition> partitions = source.listPartitions();

        Assert.assertEquals(partitions.size(), 2);
        assertBounds((MongoPartition) partitions.get(0), 0L, 1L);
        assertBounds((MongoPartition) partitions.get(1), 1L, 2L);
    }

    private static Configuration baseConfig() {
        Configuration conf = new Configuration();
        conf.put(MongoConfigKeys.GEAFLOW_DSL_MONGODB_URI, "mongodb://localhost:27017");
        conf.put(MongoConfigKeys.GEAFLOW_DSL_MONGODB_DATABASE, "geaflow");
        conf.put(MongoConfigKeys.GEAFLOW_DSL_MONGODB_COLLECTION, "records");
        return conf;
    }

    private static void assertBounds(MongoPartition partition, long lower, long upper) {
        Assert.assertEquals(partition.getLowerBound(), Long.valueOf(lower));
        Assert.assertEquals(partition.getUpperBound(), Long.valueOf(upper));
    }
}
