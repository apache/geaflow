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

package org.apache.geaflow.mcp.server;

/**
 * Alipay.com Inc
 * Copyright (c) 2004-2025 All Rights Reserved.
 *
 * @author lt on 2025/9/1.
 */
public class ToolDesc {

    public static final String createGraph = "create graph with ddl, Set the storeType to rocksdb, " +
            "ensuring the syntax is correct and do not use any syntax not present in the examples. " +
            "example: CREATE GRAPH modern (\n" +
            "\tVertex person (\n" +
            "\t  id bigint ID,\n" +
            "\t  name varchar,\n" +
            "\t  age int\n" +
            "\t),\n" +
            "\tVertex software (\n" +
            "\t  id bigint ID,\n" +
            "\t  name varchar,\n" +
            "\t  lang varchar\n" +
            "\t),\n" +
            "\tEdge knows (\n" +
            "\t  srcId bigint SOURCE ID,\n" +
            "\t  targetId bigint DESTINATION ID,\n" +
            "\t  weight double\n" +
            "\t),\n" +
            "\tEdge created (\n" +
            "\t  srcId bigint SOURCE ID,\n" +
            "  \ttargetId bigint DESTINATION ID,\n" +
            "  \tweight double\n" +
            "\t)\n" +
            ") WITH (\n" +
            "\tstoreType='rocksdb'\n" +
            ");";

    public static final String insertGraph = "Insert into graph with dml. " +
            "A single call can only insert data into one vertex or edge type, and can only use the VALUES syntax. " +
            "Do not use any syntax not present in the examples. " +
            "example: INSERT INTO modern.person(id, name, age)\n" +
            "VALUES (1, 'jim', 20), (2, 'kate', 22)\n" +
            ";";

    public static final String queryType = "query vertex or edge. " +
            "A single call can only query data from one vertex or edge type, and only one type name needs to be provided.";
}
