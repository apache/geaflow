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

package org.apache.geaflow.dsl.connector.neo4j;

import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.dsl.connector.api.TableSink;
import org.apache.geaflow.dsl.connector.api.TableSource;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class Neo4jTableConnectorTest {

    private Neo4jTableConnector connector;
    private Configuration config;

    @BeforeMethod
    public void setUp() {
        connector = new Neo4jTableConnector();
        config = new Configuration();
        config.put(Neo4jConfigKeys.GEAFLOW_DSL_NEO4J_URI, "bolt://localhost:7687");
        config.put(Neo4jConfigKeys.GEAFLOW_DSL_NEO4J_USERNAME, "neo4j");
        config.put(Neo4jConfigKeys.GEAFLOW_DSL_NEO4J_PASSWORD, "password");
    }

    @Test
    public void testGetType() {
        Assert.assertEquals(connector.getType(), "Neo4j");
    }

    @Test
    public void testCreateSource() {
        TableSource source = connector.createSource(config);
        Assert.assertNotNull(source);
        Assert.assertTrue(source instanceof Neo4jTableSource);
    }

    @Test
    public void testCreateSink() {
        TableSink sink = connector.createSink(config);
        Assert.assertNotNull(sink);
        Assert.assertTrue(sink instanceof Neo4jTableSink);
    }

    @Test
    public void testMultipleSourceInstances() {
        TableSource source1 = connector.createSource(config);
        TableSource source2 = connector.createSource(config);

        Assert.assertNotNull(source1);
        Assert.assertNotNull(source2);
        Assert.assertNotSame(source1, source2);
    }

    @Test
    public void testMultipleSinkInstances() {
        TableSink sink1 = connector.createSink(config);
        TableSink sink2 = connector.createSink(config);

        Assert.assertNotNull(sink1);
        Assert.assertNotNull(sink2);
        Assert.assertNotSame(sink1, sink2);
    }
}
