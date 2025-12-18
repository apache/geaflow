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

import static org.apache.geaflow.dsl.connector.neo4j.Neo4jConstants.DEFAULT_BATCH_SIZE;
import static org.apache.geaflow.dsl.connector.neo4j.Neo4jConstants.DEFAULT_CONNECTION_ACQUISITION_TIMEOUT_MILLIS;
import static org.apache.geaflow.dsl.connector.neo4j.Neo4jConstants.DEFAULT_DATABASE;
import static org.apache.geaflow.dsl.connector.neo4j.Neo4jConstants.DEFAULT_MAX_CONNECTION_LIFETIME_MILLIS;
import static org.apache.geaflow.dsl.connector.neo4j.Neo4jConstants.DEFAULT_MAX_CONNECTION_POOL_SIZE;
import static org.apache.geaflow.dsl.connector.neo4j.Neo4jConstants.DEFAULT_NODE_LABEL;
import static org.apache.geaflow.dsl.connector.neo4j.Neo4jConstants.DEFAULT_RELATIONSHIP_TYPE;

import org.apache.geaflow.common.config.ConfigKey;
import org.apache.geaflow.common.config.ConfigKeys;

public class Neo4jConfigKeys {

    public static final ConfigKey GEAFLOW_DSL_NEO4J_URI = ConfigKeys
        .key("geaflow.dsl.neo4j.uri")
        .noDefaultValue()
        .description("Neo4j database URI (e.g., bolt://localhost:7687).");

    public static final ConfigKey GEAFLOW_DSL_NEO4J_USERNAME = ConfigKeys
        .key("geaflow.dsl.neo4j.username")
        .noDefaultValue()
        .description("Neo4j database username.");

    public static final ConfigKey GEAFLOW_DSL_NEO4J_PASSWORD = ConfigKeys
        .key("geaflow.dsl.neo4j.password")
        .noDefaultValue()
        .description("Neo4j database password.");

    public static final ConfigKey GEAFLOW_DSL_NEO4J_DATABASE = ConfigKeys
        .key("geaflow.dsl.neo4j.database")
        .defaultValue(DEFAULT_DATABASE)
        .description("Neo4j database name.");

    public static final ConfigKey GEAFLOW_DSL_NEO4J_BATCH_SIZE = ConfigKeys
        .key("geaflow.dsl.neo4j.batch.size")
        .defaultValue(DEFAULT_BATCH_SIZE)
        .description("Batch size for writing to Neo4j.");

    public static final ConfigKey GEAFLOW_DSL_NEO4J_MAX_CONNECTION_LIFETIME = ConfigKeys
        .key("geaflow.dsl.neo4j.max.connection.lifetime.millis")
        .defaultValue(DEFAULT_MAX_CONNECTION_LIFETIME_MILLIS)
        .description("Maximum lifetime of a connection in milliseconds.");

    public static final ConfigKey GEAFLOW_DSL_NEO4J_MAX_CONNECTION_POOL_SIZE = ConfigKeys
        .key("geaflow.dsl.neo4j.max.connection.pool.size")
        .defaultValue(DEFAULT_MAX_CONNECTION_POOL_SIZE)
        .description("Maximum size of the connection pool.");

    public static final ConfigKey GEAFLOW_DSL_NEO4J_CONNECTION_ACQUISITION_TIMEOUT = ConfigKeys
        .key("geaflow.dsl.neo4j.connection.acquisition.timeout.millis")
        .defaultValue(DEFAULT_CONNECTION_ACQUISITION_TIMEOUT_MILLIS)
        .description("Timeout for acquiring a connection from the pool in milliseconds.");

    public static final ConfigKey GEAFLOW_DSL_NEO4J_QUERY = ConfigKeys
        .key("geaflow.dsl.neo4j.query")
        .noDefaultValue()
        .description("Cypher query for reading data from Neo4j.");

    public static final ConfigKey GEAFLOW_DSL_NEO4J_NODE_LABEL = ConfigKeys
        .key("geaflow.dsl.neo4j.node.label")
        .defaultValue(DEFAULT_NODE_LABEL)
        .description("Node label for writing nodes to Neo4j.");

    public static final ConfigKey GEAFLOW_DSL_NEO4J_RELATIONSHIP_TYPE = ConfigKeys
        .key("geaflow.dsl.neo4j.relationship.type")
        .defaultValue(DEFAULT_RELATIONSHIP_TYPE)
        .description("Relationship type for writing relationships to Neo4j.");

    public static final ConfigKey GEAFLOW_DSL_NEO4J_WRITE_MODE = ConfigKeys
        .key("geaflow.dsl.neo4j.write.mode")
        .defaultValue("node")
        .description("Write mode: 'node' for writing nodes, 'relationship' for writing relationships.");

    public static final ConfigKey GEAFLOW_DSL_NEO4J_NODE_ID_FIELD = ConfigKeys
        .key("geaflow.dsl.neo4j.node.id.field")
        .noDefaultValue()
        .description("Field name to use as node ID.");

    public static final ConfigKey GEAFLOW_DSL_NEO4J_RELATIONSHIP_SOURCE_FIELD = ConfigKeys
        .key("geaflow.dsl.neo4j.relationship.source.field")
        .noDefaultValue()
        .description("Field name for relationship source node ID.");

    public static final ConfigKey GEAFLOW_DSL_NEO4J_RELATIONSHIP_TARGET_FIELD = ConfigKeys
        .key("geaflow.dsl.neo4j.relationship.target.field")
        .noDefaultValue()
        .description("Field name for relationship target node ID.");
}
