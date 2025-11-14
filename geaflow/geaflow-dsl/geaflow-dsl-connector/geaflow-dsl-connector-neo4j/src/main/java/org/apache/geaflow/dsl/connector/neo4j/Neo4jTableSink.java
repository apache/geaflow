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

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.geaflow.api.context.RuntimeContext;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.common.type.IType;
import org.apache.geaflow.dsl.common.data.Row;
import org.apache.geaflow.dsl.common.exception.GeaFlowDSLException;
import org.apache.geaflow.dsl.common.types.StructType;
import org.apache.geaflow.dsl.connector.api.TableSink;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Config;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Session;
import org.neo4j.driver.SessionConfig;
import org.neo4j.driver.Transaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.geaflow.dsl.connector.neo4j.Neo4jConstants.DEFAULT_NODE_LABEL;
import static org.apache.geaflow.dsl.connector.neo4j.Neo4jConstants.DEFAULT_RELATIONSHIP_LABEL;

public class Neo4jTableSink implements TableSink {

    private static final Logger LOGGER = LoggerFactory.getLogger(Neo4jTableSink.class);

    private StructType schema;
    private String uri;
    private String username;
    private String password;
    private String database;
    private int batchSize;
    private String writeMode;
    private String nodeLabel;
    private String relationshipType;
    private String nodeIdField;
    private String relationshipSourceField;
    private String relationshipTargetField;
    private long maxConnectionLifetime;
    private int maxConnectionPoolSize;
    private long connectionAcquisitionTimeout;

    private Driver driver;
    private Session session;
    private Transaction transaction;
    private List<Row> batch;

    @Override
    public void init(Configuration tableConf, StructType schema) {
        LOGGER.info("Init Neo4j sink with config: {}, \n schema: {}", tableConf, schema);
        this.schema = schema;

        this.uri = tableConf.getString(Neo4jConfigKeys.GEAFLOW_DSL_NEO4J_URI);
        this.username = tableConf.getString(Neo4jConfigKeys.GEAFLOW_DSL_NEO4J_USERNAME);
        this.password = tableConf.getString(Neo4jConfigKeys.GEAFLOW_DSL_NEO4J_PASSWORD);
        this.database = tableConf.getString(Neo4jConfigKeys.GEAFLOW_DSL_NEO4J_DATABASE);
        this.batchSize = tableConf.getInteger(Neo4jConfigKeys.GEAFLOW_DSL_NEO4J_BATCH_SIZE);
        this.writeMode = tableConf.getString(Neo4jConfigKeys.GEAFLOW_DSL_NEO4J_WRITE_MODE);
        this.nodeLabel = tableConf.getString(Neo4jConfigKeys.GEAFLOW_DSL_NEO4J_NODE_LABEL);
        this.relationshipType = tableConf.getString(Neo4jConfigKeys.GEAFLOW_DSL_NEO4J_RELATIONSHIP_TYPE);
        this.nodeIdField = tableConf.getString(Neo4jConfigKeys.GEAFLOW_DSL_NEO4J_NODE_ID_FIELD);
        this.relationshipSourceField = tableConf.getString(Neo4jConfigKeys.GEAFLOW_DSL_NEO4J_RELATIONSHIP_SOURCE_FIELD);
        this.relationshipTargetField = tableConf.getString(Neo4jConfigKeys.GEAFLOW_DSL_NEO4J_RELATIONSHIP_TARGET_FIELD);
        this.maxConnectionLifetime = tableConf.getLong(Neo4jConfigKeys.GEAFLOW_DSL_NEO4J_MAX_CONNECTION_LIFETIME);
        this.maxConnectionPoolSize = tableConf.getInteger(Neo4jConfigKeys.GEAFLOW_DSL_NEO4J_MAX_CONNECTION_POOL_SIZE);
        this.connectionAcquisitionTimeout = tableConf.getLong(Neo4jConfigKeys.GEAFLOW_DSL_NEO4J_CONNECTION_ACQUISITION_TIMEOUT);

        validateConfig();
        this.batch = new ArrayList<>(batchSize);
    }

    private void validateConfig() {
        if (uri == null || uri.isEmpty()) {
            throw new GeaFlowDSLException("Neo4j URI must be specified");
        }
        if (username == null || username.isEmpty()) {
            throw new GeaFlowDSLException("Neo4j username must be specified");
        }
        if (password == null || password.isEmpty()) {
            throw new GeaFlowDSLException("Neo4j password must be specified");
        }
        if (DEFAULT_NODE_LABEL.toLowerCase().equals(writeMode)) {
            if (nodeIdField == null || nodeIdField.isEmpty()) {
                throw new GeaFlowDSLException("Node ID field must be specified for node write mode");
            }
        } else if (DEFAULT_RELATIONSHIP_LABEL.equals(writeMode)) {
            if (relationshipSourceField == null || relationshipSourceField.isEmpty() 
                || relationshipTargetField == null || relationshipTargetField.isEmpty()) {
                throw new GeaFlowDSLException("Relationship source and target fields must be specified for relationship write mode");
            }
        } else {
            throw new GeaFlowDSLException("Invalid write mode: " + writeMode + ". Must be 'node' or 'relationship'");
        }
    }

    @Override
    public void open(RuntimeContext context) {
        try {
            Config config = Config.builder()
                .withMaxConnectionLifetime(maxConnectionLifetime, TimeUnit.MILLISECONDS)
                .withMaxConnectionPoolSize(maxConnectionPoolSize)
                .withConnectionAcquisitionTimeout(connectionAcquisitionTimeout, TimeUnit.MILLISECONDS)
                .build();

            this.driver = GraphDatabase.driver(uri, AuthTokens.basic(username, password), config);
            
            SessionConfig sessionConfig = SessionConfig.builder()
                .withDatabase(database)
                .build();
            
            this.session = driver.session(sessionConfig);
            this.transaction = session.beginTransaction();
            
            LOGGER.info("Neo4j connection established successfully");
        } catch (Exception e) {
            throw new GeaFlowDSLException("Failed to connect to Neo4j: " + e.getMessage(), e);
        }
    }

    @Override
    public void write(Row row) throws IOException {
        batch.add(row);
        if (batch.size() >= batchSize) {
            flush();
        }
    }

    @Override
    public void finish() throws IOException {
        if (!batch.isEmpty()) {
            flush();
        }
        try {
            if (transaction != null) {
                transaction.commit();
                transaction.close();
                transaction = null;
            }
        } catch (Exception e) {
            LOGGER.error("Failed to commit transaction", e);
            try {
                if (transaction != null) {
                    transaction.rollback();
                }
            } catch (Exception ex) {
                throw new GeaFlowDSLException("Failed to rollback transaction", ex);
            }
            throw new GeaFlowDSLException("Failed to finish writing to Neo4j", e);
        }
    }

    @Override
    public void close() {
        try {
            if (transaction != null) {
                transaction.close();
                transaction = null;
            }
            if (session != null) {
                session.close();
                session = null;
            }
            if (driver != null) {
                driver.close();
                driver = null;
            }
            LOGGER.info("Neo4j connection closed successfully");
        } catch (Exception e) {
            throw new GeaFlowDSLException("Failed to close Neo4j connection", e);
        }
    }

    private void flush() {
        if (batch.isEmpty()) {
            return;
        }

        try {
            if (DEFAULT_NODE_LABEL.toLowerCase().equals(writeMode)) {
                writeNodes();
            } else {
                writeRelationships();
            }
            batch.clear();
        } catch (Exception e) {
            throw new GeaFlowDSLException("Failed to flush batch to Neo4j", e);
        }
    }

    private void writeNodes() {
        List<String> fieldNames = schema.getFieldNames();
        IType<?>[] types = schema.getTypes();
        
        int nodeIdIndex = fieldNames.indexOf(nodeIdField);
        if (nodeIdIndex == -1) {
            throw new GeaFlowDSLException("Node ID field not found in schema: " + nodeIdField);
        }

        for (Row row : batch) {
            Map<String, Object> properties = new HashMap<>();
            for (int i = 0; i < fieldNames.size(); i++) {
                if (i == nodeIdIndex) {
                    continue; // Skip ID field, it will be used as node ID
                }
                Object value = row.getField(i, types[i]);
                if (value != null) {
                    properties.put(fieldNames.get(i), value);
                }
            }

            Object nodeId = row.getField(nodeIdIndex, types[nodeIdIndex]);
            if (nodeId == null) {
                throw new GeaFlowDSLException("Node ID cannot be null");
            }

            String cypher = String.format(
                "MERGE (n:%s {id: $id}) SET n += $properties",
                nodeLabel
            );
            
            Map<String, Object> parameters = new HashMap<>();
            parameters.put("id", nodeId);
            parameters.put("properties", properties);
            
            transaction.run(cypher, parameters);
        }
    }

    private void writeRelationships() {
        List<String> fieldNames = schema.getFieldNames();
        IType<?>[] types = schema.getTypes();
        
        int sourceIndex = fieldNames.indexOf(relationshipSourceField);
        int targetIndex = fieldNames.indexOf(relationshipTargetField);
        
        if (sourceIndex == -1) {
            throw new GeaFlowDSLException("Relationship source field not found in schema: " + relationshipSourceField);
        }
        if (targetIndex == -1) {
            throw new GeaFlowDSLException("Relationship target field not found in schema: " + relationshipTargetField);
        }

        for (Row row : batch) {
            Object sourceId = row.getField(sourceIndex, types[sourceIndex]);
            Object targetId = row.getField(targetIndex, types[targetIndex]);
            
            if (sourceId == null || targetId == null) {
                throw new GeaFlowDSLException("Relationship source and target IDs cannot be null");
            }

            Map<String, Object> properties = new HashMap<>();
            for (int i = 0; i < fieldNames.size(); i++) {
                if (i == sourceIndex || i == targetIndex) {
                    continue; // Skip source and target fields
                }
                Object value = row.getField(i, types[i]);
                if (value != null) {
                    properties.put(fieldNames.get(i), value);
                }
            }

            final String cypher = String.format(
                "MATCH (a {id: $sourceId}), (b {id: $targetId}) "
                + "MERGE (a)-[r:%s]->(b) SET r += $properties",
                relationshipType
            );
            
            Map<String, Object> parameters = new HashMap<>();
            parameters.put("sourceId", sourceId);
            parameters.put("targetId", targetId);
            parameters.put("properties", properties);
            
            transaction.run(cypher, parameters);
        }
    }
}
