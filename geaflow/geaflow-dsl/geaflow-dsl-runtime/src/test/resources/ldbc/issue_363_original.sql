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

-- Issue #363: Original Query (with redundancy)
-- This query has performance issues due to:
-- 1. Variable 'a' is declared twice
-- 2. Unclear join path
-- 3. Potential cartesian product

USE GRAPH bi;

CREATE TABLE issue_363_original_result (
  a_id bigint,
  b_id bigint,
  c_id bigint,
  d_id bigint
) WITH (
  type='file',
  geaflow.dsl.file.path='${target}'
);

-- Original query from Issue #363 (fixed to be executable)
-- Note: This represents a "less optimized" pattern with separate MATCH clauses
INSERT INTO issue_363_original_result
SELECT
  a_id,
  b_id,
  c_id,
  d_id
FROM (
  MATCH
    (a:Person where a.id = 1100001)<-[e:hasCreator]-(b),
    (c:Person) -[knows1:knows]-> (d:Person where d.id = 1100005),
    (a) <-[knows2:knows]- (c)
  RETURN a.id as a_id, b.id as b_id, c.id as c_id, d.id as d_id
  ORDER BY a_id, b_id, c_id, d_id
);
