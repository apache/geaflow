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

package org.apache.geaflow.dsl.connector.elasticsearch;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import org.apache.geaflow.api.context.RuntimeContext;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.common.type.Types;
import org.apache.geaflow.dsl.common.data.Row;
import org.apache.geaflow.dsl.common.data.impl.ObjectRow;
import org.apache.geaflow.dsl.common.types.StructType;
import org.apache.geaflow.dsl.common.types.TableField;
import org.apache.geaflow.dsl.common.types.TableSchema;
import org.apache.geaflow.dsl.connector.api.FetchData;
import org.apache.geaflow.dsl.connector.api.Partition;
import org.apache.geaflow.dsl.connector.api.window.SizeFetchWindow;
import org.mockito.Mockito;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.elasticsearch.ElasticsearchContainer;
import org.testcontainers.utility.DockerImageName;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class ElasticsearchIntegrationTest {

    private static final String ELASTICSEARCH_VERSION = "8.11.0";
    private ElasticsearchContainer container;
    private Configuration config;
    private StructType sinkSchema;
    private TableSchema sourceSchema;

    @BeforeClass
    public void setUp() {
        // Check if Docker is available
        try {
            ProcessBuilder pb = new ProcessBuilder("docker", "info");
            Process process = pb.start();
            int exitCode = process.waitFor();
            if (exitCode != 0) {
                throw new RuntimeException("Docker is not running");
            }
        } catch (Exception e) {
            throw new RuntimeException("Docker is not available, skipping test", e);
        }

        DockerImageName image = DockerImageName
                .parse("docker.elastic.co/elasticsearch/elasticsearch")
                .withTag(ELASTICSEARCH_VERSION);

        container = new ElasticsearchContainer(image)
                .withEnv("xpack.security.enabled", "false")
                .withEnv("discovery.type", "single-node")
                .waitingFor(Wait.forHttp("/_cluster/health")
                        .forStatusCode(200));

        container.start();

        // Configure connection
        config = new Configuration();
        config.put(ElasticsearchConfigKeys.GEAFLOW_DSL_ELASTICSEARCH_HOSTS,
                container.getHttpHostAddress());
        config.put(ElasticsearchConfigKeys.GEAFLOW_DSL_ELASTICSEARCH_INDEX, "test_users");
        config.put(ElasticsearchConfigKeys.GEAFLOW_DSL_ELASTICSEARCH_DOCUMENT_ID_FIELD, "id");
        config.put(ElasticsearchConfigKeys.GEAFLOW_DSL_ELASTICSEARCH_BATCH_SIZE, "10");

        // Create schema
        TableField idField = new TableField("id", Types.INTEGER, false);
        TableField nameField = new TableField("name", Types.STRING, false);
        TableField ageField = new TableField("age", Types.INTEGER, false);
        sinkSchema = new StructType(Arrays.asList(idField, nameField, ageField));
        sourceSchema = new TableSchema(sinkSchema);
    }

    @AfterClass
    public void tearDown() {
        if (container != null) {
            container.stop();
        }
    }

    @Test
    public void testWriteAndRead() throws IOException {
        RuntimeContext context = Mockito.mock(RuntimeContext.class);

        // Write data
        ElasticsearchTableSink sink = new ElasticsearchTableSink();
        sink.init(config, sinkSchema);
        sink.open(context);

        for (int i = 1; i <= 5; i++) {
            Row row = ObjectRow.create(i, "User" + i, 20 + i);
            sink.write(row);
        }
        sink.finish();
        sink.close();

        // Wait for indexing
        try {
            Thread.sleep(2000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        // Read data
        ElasticsearchTableSource source = new ElasticsearchTableSource();
        source.init(config, sourceSchema);
        source.open(context);

        List<Partition> partitions = source.listPartitions();
        Assert.assertEquals(partitions.size(), 1);

        FetchData<Row> fetchData = source.fetch(partitions.get(0), Optional.empty(),
                new SizeFetchWindow(0L, 10L));
        Assert.assertNotNull(fetchData);

        Iterator<Row> rowIterator = fetchData.getDataIterator();
        Assert.assertNotNull(rowIterator);
        List<Row> rows = new ArrayList<>();
        rowIterator.forEachRemaining(rows::add);
        Assert.assertTrue(rows.size() > 0);

        source.close();
    }

    @Test
    public void testBulkWrite() throws IOException {
        RuntimeContext context = Mockito.mock(RuntimeContext.class);

        ElasticsearchTableSink sink = new ElasticsearchTableSink();
        sink.init(config, sinkSchema);
        sink.open(context);

        // Write more than batch size
        for (int i = 1; i <= 25; i++) {
            Row row = ObjectRow.create(i, "BulkUser" + i, 30 + i);
            sink.write(row);
        }

        sink.finish();
        sink.close();
    }

    @Test
    public void testQueryDsl() throws IOException {
        RuntimeContext context = Mockito.mock(RuntimeContext.class);

        // First write some data
        ElasticsearchTableSink sink = new ElasticsearchTableSink();
        config.put(ElasticsearchConfigKeys.GEAFLOW_DSL_ELASTICSEARCH_INDEX, "test_query");
        sink.init(config, sinkSchema);
        sink.open(context);

        Row row1 = ObjectRow.create(1, "Alice", 25);
        Row row2 = ObjectRow.create(2, "Bob", 30);
        sink.write(row1);
        sink.write(row2);
        sink.finish();
        sink.close();

        // Wait for indexing
        try {
            Thread.sleep(2000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        // Read with query
        ElasticsearchTableSource source = new ElasticsearchTableSource();
        source.init(config, sourceSchema);
        source.open(context);

        List<Partition> partitions = source.listPartitions();
        FetchData<Row> fetchData = source.fetch(partitions.get(0), Optional.empty(),
                new SizeFetchWindow(0L, 10L));

        Assert.assertNotNull(fetchData);
        source.close();
    }
}
