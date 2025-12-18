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

import org.testng.Assert;
import org.testng.annotations.Test;

public class Neo4jConfigKeysTest {

    @Test
    public void testDefaultValues() {
        Assert.assertEquals(Neo4jConstants.DEFAULT_DATABASE, "neo4j");
        Assert.assertEquals(Neo4jConstants.DEFAULT_BATCH_SIZE, 1000);
        Assert.assertEquals(Neo4jConstants.DEFAULT_MAX_CONNECTION_LIFETIME_MILLIS, 3600000L);
        Assert.assertEquals(Neo4jConstants.DEFAULT_MAX_CONNECTION_POOL_SIZE, 100);
        Assert.assertEquals(Neo4jConstants.DEFAULT_CONNECTION_ACQUISITION_TIMEOUT_MILLIS, 60000L);
        Assert.assertEquals(Neo4jConstants.DEFAULT_NODE_LABEL, "Node");
        Assert.assertEquals(Neo4jConstants.DEFAULT_RELATIONSHIP_TYPE, "RELATES_TO");
    }

    @Test
    public void testConfigKeyNames() {
        Assert.assertEquals(Neo4jConfigKeys.GEAFLOW_DSL_NEO4J_URI.getKey(), 
            "geaflow.dsl.neo4j.uri");
        Assert.assertEquals(Neo4jConfigKeys.GEAFLOW_DSL_NEO4J_USERNAME.getKey(), 
            "geaflow.dsl.neo4j.username");
        Assert.assertEquals(Neo4jConfigKeys.GEAFLOW_DSL_NEO4J_PASSWORD.getKey(), 
            "geaflow.dsl.neo4j.password");
        Assert.assertEquals(Neo4jConfigKeys.GEAFLOW_DSL_NEO4J_DATABASE.getKey(), 
            "geaflow.dsl.neo4j.database");
        Assert.assertEquals(Neo4jConfigKeys.GEAFLOW_DSL_NEO4J_BATCH_SIZE.getKey(), 
            "geaflow.dsl.neo4j.batch.size");
        Assert.assertEquals(Neo4jConfigKeys.GEAFLOW_DSL_NEO4J_QUERY.getKey(), 
            "geaflow.dsl.neo4j.query");
        Assert.assertEquals(Neo4jConfigKeys.GEAFLOW_DSL_NEO4J_NODE_LABEL.getKey(), 
            "geaflow.dsl.neo4j.node.label");
        Assert.assertEquals(Neo4jConfigKeys.GEAFLOW_DSL_NEO4J_RELATIONSHIP_TYPE.getKey(), 
            "geaflow.dsl.neo4j.relationship.type");
        Assert.assertEquals(Neo4jConfigKeys.GEAFLOW_DSL_NEO4J_WRITE_MODE.getKey(), 
            "geaflow.dsl.neo4j.write.mode");
        Assert.assertEquals(Neo4jConfigKeys.GEAFLOW_DSL_NEO4J_NODE_ID_FIELD.getKey(), 
            "geaflow.dsl.neo4j.node.id.field");
        Assert.assertEquals(Neo4jConfigKeys.GEAFLOW_DSL_NEO4J_RELATIONSHIP_SOURCE_FIELD.getKey(), 
            "geaflow.dsl.neo4j.relationship.source.field");
        Assert.assertEquals(Neo4jConfigKeys.GEAFLOW_DSL_NEO4J_RELATIONSHIP_TARGET_FIELD.getKey(), 
            "geaflow.dsl.neo4j.relationship.target.field");
    }

    @Test
    public void testConfigKeyDefaults() {
        Assert.assertEquals(Neo4jConfigKeys.GEAFLOW_DSL_NEO4J_DATABASE.getDefaultValue(), 
            Neo4jConstants.DEFAULT_DATABASE);
        Assert.assertEquals(Neo4jConfigKeys.GEAFLOW_DSL_NEO4J_BATCH_SIZE.getDefaultValue(), 
            Neo4jConstants.DEFAULT_BATCH_SIZE);
        Assert.assertEquals(Neo4jConfigKeys.GEAFLOW_DSL_NEO4J_NODE_LABEL.getDefaultValue(), 
            Neo4jConstants.DEFAULT_NODE_LABEL);
        Assert.assertEquals(Neo4jConfigKeys.GEAFLOW_DSL_NEO4J_RELATIONSHIP_TYPE.getDefaultValue(), 
            Neo4jConstants.DEFAULT_RELATIONSHIP_TYPE);
        Assert.assertEquals(Neo4jConfigKeys.GEAFLOW_DSL_NEO4J_WRITE_MODE.getDefaultValue(), 
            "node");
    }
}
