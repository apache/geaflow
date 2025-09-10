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

-- Test Case: shared with Edge Direction Conditions
-- Purpose: Verify shared functionality with different edge directions
-- Query: (a:person) -[e1:knows]-> (b) | (a:person) <-[e2:created]- (c) WHERE SHARED(e1.weight = e2.weight)
-- Description: This test validates that shared can handle conditions comparing
-- edge properties from different directions (outgoing and incoming edges).
-- It ensures that edge direction is properly considered when evaluating shared conditions.
-- Expected: Returns person vertices where knows edge weight equals created edge weight

CREATE TABLE tbl_result (
  a_id bigint,
  e1_weight double,
  b_id bigint,
  e2_weight double,
  c_id bigint
) WITH (
	type='file',
	geaflow.dsl.file.path='${target}'
);

USE GRAPH modern;

INSERT INTO tbl_result
SELECT
	a_id,
  e1_weight,
  b_id,
  e2_weight,
  c_id
FROM (
  MATCH (a:person) -[e1:knows]-> (b) | (a:person) <-[e2:created]- (c) WHERE SHARED(e1.weight = e2.weight)
  RETURN a.id as a_id, e1.weight as e1_weight, b.id as b_id, e2.weight as e2_weight, c.id as c_id
)
