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

-- GraphSAGE test query
-- Note: GraphSAGE is implemented as IncVertexCentricCompute, not as a CALL algorithm
-- This query demonstrates how to use GraphSAGE through graph computation
-- The actual execution is handled by the test class

CREATE TABLE tbl_result (
  vid bigint,
  embedding varchar  -- JSON string representing List<Double> embedding
) WITH (
	type='file',
	geaflow.dsl.file.path='${target}'
);

USE GRAPH graphsage_test;

-- This is a placeholder query structure
-- The actual GraphSAGE computation is performed by the test class
-- which directly uses GraphSAGECompute with IncGraphView.incrementalCompute()

SELECT id as vid, name
FROM node
LIMIT 10
;

