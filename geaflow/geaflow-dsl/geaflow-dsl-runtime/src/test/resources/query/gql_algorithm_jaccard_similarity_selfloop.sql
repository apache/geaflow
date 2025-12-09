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

CREATE TABLE result_tb (
  vertex_a int,
  vertex_b int,
  jaccard_coefficient double
) WITH (
     type='file',
     geaflow.dsl.file.path='${target}'
);

USE GRAPH jaccard_test;

INSERT INTO result_tb
CALL jaccard_similarity(9, 9) YIELD (vertex_a, vertex_b, jaccard_coefficient)
RETURN cast(vertex_a as int), cast(vertex_b as int), jaccard_coefficient
;
