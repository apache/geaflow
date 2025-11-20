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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.apache.geaflow.api.context.RuntimeContext;
import org.apache.geaflow.api.window.WindowType;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.dsl.common.data.Row;
import org.apache.geaflow.dsl.common.data.impl.ObjectRow;
import org.apache.geaflow.dsl.common.exception.GeaFlowDSLException;
import org.apache.geaflow.dsl.common.types.StructType;
import org.apache.geaflow.dsl.common.types.TableSchema;
import org.apache.geaflow.dsl.connector.api.FetchData;
import org.apache.geaflow.dsl.connector.api.Offset;
import org.apache.geaflow.dsl.connector.api.Partition;
import org.apache.geaflow.dsl.connector.api.TableSource;
import org.apache.geaflow.dsl.connector.api.serde.DeserializerFactory;
import org.apache.geaflow.dsl.connector.api.serde.TableDeserializer;
import org.apache.geaflow.dsl.connector.api.window.FetchWindow;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Config;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Record;
import org.neo4j.driver.Result;
import org.neo4j.driver.Session;
import org.neo4j.driver.SessionConfig;
import org.neo4j.driver.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Neo4jTableSource implements TableSource {

    private static final Logger LOGGER = LoggerFactory.getLogger(Neo4jTableSource.class);

    private Configuration tableConf;
    private StructType schema;
    private String uri;
    private String username;
    private String password;
    private String database;
    private String cypherQuery;
    private long maxConnectionLifetime;
    private int maxConnectionPoolSize;
    private long connectionAcquisitionTimeout;

    private Driver driver;
    private Map<Partition, Session> partitionSessionMap = new HashMap<>();

    @Override
    public void init(Configuration tableConf, TableSchema tableSchema) {
        LOGGER.info("Init Neo4j source with config: {}, \n schema: {}", tableConf, tableSchema);
        this.tableConf = tableConf;
        this.schema = tableSchema;

        this.uri = tableConf.getString(Neo4jConfigKeys.GEAFLOW_DSL_NEO4J_URI);
        this.username = tableConf.getString(Neo4jConfigKeys.GEAFLOW_DSL_NEO4J_USERNAME);
        this.password = tableConf.getString(Neo4jConfigKeys.GEAFLOW_DSL_NEO4J_PASSWORD);
        this.database = tableConf.getString(Neo4jConfigKeys.GEAFLOW_DSL_NEO4J_DATABASE);
        this.cypherQuery = tableConf.getString(Neo4jConfigKeys.GEAFLOW_DSL_NEO4J_QUERY);
        this.maxConnectionLifetime = tableConf.getLong(Neo4jConfigKeys.GEAFLOW_DSL_NEO4J_MAX_CONNECTION_LIFETIME);
        this.maxConnectionPoolSize = tableConf.getInteger(Neo4jConfigKeys.GEAFLOW_DSL_NEO4J_MAX_CONNECTION_POOL_SIZE);
        this.connectionAcquisitionTimeout = tableConf.getLong(Neo4jConfigKeys.GEAFLOW_DSL_NEO4J_CONNECTION_ACQUISITION_TIMEOUT);

        if (cypherQuery == null || cypherQuery.isEmpty()) {
            throw new GeaFlowDSLException("Neo4j query must be specified");
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
            LOGGER.info("Neo4j driver created successfully");
        } catch (Exception e) {
            throw new GeaFlowDSLException("Failed to create Neo4j driver: " + e.getMessage(), e);
        }
    }

    @Override
    public List<Partition> listPartitions() {
        // Neo4j doesn't have native partitioning like JDBC
        // For simplicity, we return a single partition
        return Collections.singletonList(new Neo4jPartition(cypherQuery));
    }

    @Override
    public List<Partition> listPartitions(int parallelism) {
        return listPartitions();
    }

    @Override
    public <IN> TableDeserializer<IN> getDeserializer(Configuration conf) {
        return DeserializerFactory.loadRowTableDeserializer();
    }

    @Override
    public <T> FetchData<T> fetch(Partition partition, Optional<Offset> startOffset,
                                  FetchWindow windowInfo) throws IOException {
        if (!(windowInfo.getType() == WindowType.SIZE_TUMBLING_WINDOW
            || windowInfo.getType() == WindowType.ALL_WINDOW)) {
            throw new GeaFlowDSLException("Not support window type: {}", windowInfo.getType());
        }

        Neo4jPartition neo4jPartition = (Neo4jPartition) partition;
        Session session = partitionSessionMap.get(partition);
        
        if (session == null) {
            SessionConfig sessionConfig = SessionConfig.builder()
                .withDatabase(database)
                .build();
            session = driver.session(sessionConfig);
            partitionSessionMap.put(partition, session);
        }

        long offset = startOffset.isPresent() ? startOffset.get().getOffset() : 0;
        
        List<Row> dataList = new ArrayList<>();
        try {
            String query = neo4jPartition.getQuery();
            // Add SKIP and LIMIT to the query for pagination
            String paginatedQuery = query + " SKIP $skip LIMIT $limit";
            
            Map<String, Object> parameters = new HashMap<>();
            parameters.put("skip", offset);
            parameters.put("limit", windowInfo.windowSize());
            
            Result result = session.run(paginatedQuery, parameters);
            
            List<String> fieldNames = schema.getFieldNames();
            
            while (result.hasNext()) {
                Record record = result.next();
                Object[] values = new Object[fieldNames.size()];
                
                for (int i = 0; i < fieldNames.size(); i++) {
                    String fieldName = fieldNames.get(i);
                    if (record.containsKey(fieldName)) {
                        Value value = record.get(fieldName);
                        values[i] = convertNeo4jValue(value);
                    } else {
                        values[i] = null;
                    }
                }
                
                dataList.add(ObjectRow.create(values));
            }
            
        } catch (Exception e) {
            throw new GeaFlowDSLException("Failed to fetch data from Neo4j", e);
        }

        Neo4jOffset nextOffset = new Neo4jOffset(offset + dataList.size());
        boolean isFinish = windowInfo.getType() == WindowType.ALL_WINDOW 
            || dataList.size() < windowInfo.windowSize();
        
        return (FetchData<T>) FetchData.createStreamFetch(dataList, nextOffset, isFinish);
    }

    @Override
    public void close() {
        try {
            for (Session session : partitionSessionMap.values()) {
                if (session != null) {
                    session.close();
                }
            }
            partitionSessionMap.clear();
            
            if (driver != null) {
                driver.close();
                driver = null;
            }
            LOGGER.info("Neo4j connections closed successfully");
        } catch (Exception e) {
            throw new GeaFlowDSLException("Failed to close Neo4j connections", e);
        }
    }

    private Object convertNeo4jValue(Value value) {
        if (value.isNull()) {
            return null;
        }
        
        switch (value.type().name()) {
            case "INTEGER":
                return value.asLong();
            case "FLOAT":
                return value.asDouble();
            case "STRING":
                return value.asString();
            case "BOOLEAN":
                return value.asBoolean();
            case "LIST":
                return value.asList();
            case "MAP":
                return value.asMap();
            case "NODE":
                return value.asNode().asMap();
            case "RELATIONSHIP":
                return value.asRelationship().asMap();
            case "PATH":
                return value.asPath().toString();
            default:
                return value.asObject();
        }
    }

    public static class Neo4jPartition implements Partition {

        private final String query;

        public Neo4jPartition(String query) {
            this.query = query;
        }

        public String getQuery() {
            return query;
        }

        @Override
        public String getName() {
            return "neo4j-partition-" + query.hashCode();
        }

        @Override
        public int hashCode() {
            return Objects.hash(query);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof Neo4jPartition)) {
                return false;
            }
            Neo4jPartition that = (Neo4jPartition) o;
            return Objects.equals(query, that.query);
        }
    }

    public static class Neo4jOffset implements Offset {

        private final long offset;

        public Neo4jOffset(long offset) {
            this.offset = offset;
        }

        @Override
        public String humanReadable() {
            return String.valueOf(offset);
        }

        @Override
        public long getOffset() {
            return offset;
        }

        @Override
        public boolean isTimestamp() {
            return false;
        }
    }
}
