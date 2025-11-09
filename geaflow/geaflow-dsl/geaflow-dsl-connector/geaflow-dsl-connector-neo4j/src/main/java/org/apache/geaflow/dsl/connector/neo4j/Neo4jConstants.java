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

public class Neo4jConstants {

    public static final String DEFAULT_DATABASE = "neo4j";

    public static final int DEFAULT_BATCH_SIZE = 1000;

    public static final long DEFAULT_MAX_CONNECTION_LIFETIME_MILLIS = 3600000L; // 1 hour

    public static final int DEFAULT_MAX_CONNECTION_POOL_SIZE = 100;

    public static final long DEFAULT_CONNECTION_ACQUISITION_TIMEOUT_MILLIS = 60000L; // 1 minute

    public static final String DEFAULT_NODE_LABEL = "Node";

    public static final String DEFAULT_RELATIONSHIP_TYPE = "RELATES_TO";
}
