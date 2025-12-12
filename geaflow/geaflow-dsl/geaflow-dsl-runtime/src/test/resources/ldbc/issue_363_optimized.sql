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

-- Issue #363: Optimized Query
-- This query improves performance by:
-- 1. Eliminating redundant variable declaration (a declared only once)
-- 2. Clear join path: a <- c -> d
-- 3. Consolidated WHERE clause for better optimization

USE GRAPH bi;

CREATE TABLE issue_363_optimized_result (
  a_id bigint,
  b_id bigint,
  c_id bigint,
  d_id bigint
) WITH (
  type='file',
  geaflow.dsl.file.path='${target}'
);

-- Optimized query for Issue #363
INSERT INTO issue_363_optimized_result
SELECT
  a_id,
  b_id,
  c_id,
  d_id
FROM (
  MATCH
    (a:Person)<-[e:hasCreator]-(b),
    (a)<-[knows1:knows]-(c:Person)-[knows2:knows]->(d:Person)
  WHERE a.id = 1100001 AND d.id = 1100005
  RETURN a.id as a_id, b.id as b_id, c.id as c_id, d.id as d_id
  ORDER BY a_id, b_id, c_id, d_id
);
