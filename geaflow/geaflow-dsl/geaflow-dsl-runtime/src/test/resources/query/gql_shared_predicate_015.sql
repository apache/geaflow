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

-- Test Case: Same Predicate with Subquery Integration
-- Purpose: Verify Same Predicate functionality with subquery conditions
-- Query: (a:person) -> (b) | (a:person) -> (c) WHERE SHARED(a.id IN (SELECT id FROM person WHERE age > 25))
-- Description: This test validates that Same Predicate can handle subquery conditions
-- properly. It ensures that subqueries are correctly evaluated and that the result
-- is properly applied to both path patterns in the Same Predicate.
-- Expected: Returns person vertices whose IDs are in the subquery result and their connected vertices

CREATE TABLE tbl_result (
  a_id bigint,
  a_age int,
  b_id bigint,
  c_id bigint
) WITH (
	type='file',
	geaflow.dsl.file.path='${target}'
);

USE GRAPH modern;

INSERT INTO tbl_result
SELECT
	a_id,
  a_age,
  b_id,
  c_id
FROM (
  MATCH (a:person) -> (b) | (a:person) -> (c) WHERE SHARED(a.id IN (SELECT id FROM person WHERE age > 25))
  RETURN a.id as a_id, a.age as a_age, b.id as b_id, c.id as c_id
)
