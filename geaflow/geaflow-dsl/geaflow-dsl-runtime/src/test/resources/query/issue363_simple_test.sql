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

-- Test query for Issue #363 optimization rules
-- This query tests:
-- 1. IdFilterPushdownRule: Pushes "a.id = 1" filter to VertexMatch
-- 2. AnchorNodePriorityRule: Recognizes 'a' as anchor node
-- 3. GraphJoinReorderRule: Optimizes join order based on selectivity

CREATE TABLE issue363_simple_result (
  a_id bigint,
  a_name varchar,
  b_id bigint,
  b_name varchar
) WITH (
  type='file',
  geaflow.dsl.file.path='${target}'
);

USE GRAPH issue363_simple;

INSERT INTO issue363_simple_result
SELECT
  a_id,
  a_name,
  b_id,
  b_name
FROM (
  MATCH (a:Person where a.id = 1)-[knows]->(b:Person)
  RETURN a.id as a_id, a.name as a_name, b.id as b_id, b.name as b_name
  ORDER BY a_id, b_id
);


