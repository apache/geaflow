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

-- Test Case: shared with Function Calls
-- Purpose: Verify shared functionality with built-in functions
-- Query: (a:person) -> (b) | (a:person) -> (c) WHERE SHARED(LENGTH(a.name) > 4)
-- Description: This test validates that shared can handle built-in function calls
-- in conditions. It ensures that functions like LENGTH() are properly evaluated
-- and that the function result is correctly applied to both path patterns.
-- Expected: Returns person vertices with name length > 4 and their connected vertices

CREATE TABLE tbl_result (
  a_id bigint,
  a_name varchar,
  name_length int,
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
  a_name,
  name_length,
  b_id,
  c_id
FROM (
  MATCH (a:person) -> (b) | (a:person) -> (c) WHERE SHARED(LENGTH(a.name) > 4)
  RETURN a.id as a_id, a.name as a_name, LENGTH(a.name) as name_length, b.id as b_id, c.id as c_id
)
