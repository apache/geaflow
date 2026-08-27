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

-- Issue #363: SF1 dataset setup query
--
-- This query is used to trigger graph creation + ingestion in test @BeforeClass,
-- so later benchmark iterations can measure query execution without ingestion time.

USE GRAPH bi;

CREATE TABLE issue_363_sf1_setup_result (
  a_id bigint
) WITH (
  type='file',
  geaflow.dsl.file.path='${target}'
);

INSERT INTO issue_363_sf1_setup_result
SELECT a_id
FROM (
  MATCH (a:Person where a.id = ${issue363_a_id})
  RETURN a.id as a_id
);
