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

-- LDBC BI Graph Schema for Issue #363 (SF1 dataset) - DDL ONLY
--
-- This file only defines the graph schema and does NOT ingest data.
-- It is used by SF1 benchmark tests to re-register the graph in a fresh QueryTester session
-- without repeating the expensive ingestion step.

CREATE GRAPH bi (
  Vertex Person (
    id bigint ID,
    creationDate varchar,
    firstName varchar,
    lastName varchar,
    gender varchar,
    browserUsed varchar,
    locationIP varchar
  ),
  Vertex Post (
    id bigint ID,
    creationDate varchar,
    browserUsed varchar,
    locationIP varchar,
    content varchar,
    length bigint,
    lang varchar,
    imageFile varchar
  ),
  Vertex Comment (
    id bigint ID,
    creationDate varchar,
    browserUsed varchar,
    locationIP varchar,
    content varchar,
    length bigint
  ),

  Edge knows (
    srcId bigint SOURCE ID,
    targetId bigint DESTINATION ID,
    creationDate varchar
  ),
  Edge hasCreator (
    srcId bigint SOURCE ID,
    targetId bigint DESTINATION ID
  )
) WITH (
  storeType='rocksdb',
  shardCount=${issue363_sf1_shard_count}
);

