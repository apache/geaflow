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

CREATE FUNCTION temporal_batch AS 'org.apache.geaflow.ai.temporal.udga.TemporalBatchUdga';

CREATE GRAPH temporal_business (
  Vertex event (id varchar ID, payload varchar, seed varchar),
  Vertex fact_key (id varchar ID, payload varchar, seed varchar),
  Edge applies_to (src varchar SOURCE ID, dst varchar DESTINATION ID),
  Edge candidate (src varchar SOURCE ID, dst varchar DESTINATION ID)
) WITH (storeType = 'memory', shardCount = 1);

CREATE TABLE fact_input (id varchar, payload varchar, seed varchar)
WITH (type = 'file', geaflow.dsl.file.path = '${root}/facts.csv', geaflow.dsl.window.size = -1);
CREATE TABLE event_input (id varchar, payload varchar, seed varchar)
WITH (type = 'file', geaflow.dsl.file.path = '${root}/events.csv', geaflow.dsl.window.size = -1);
CREATE TABLE edge_input (src varchar, dst varchar)
WITH (type = 'file', geaflow.dsl.file.path = '${root}/edges.csv',
      geaflow.dsl.window.size = ${windowSize});

INSERT INTO temporal_business.fact_key SELECT id, payload, seed FROM fact_input;
INSERT INTO temporal_business.event SELECT id, payload, seed FROM event_input;
INSERT INTO temporal_business.applies_to SELECT src, dst FROM edge_input;

CREATE TABLE results (vertex_id varchar, state_payload varchar, phase varchar)
WITH (type = 'file', geaflow.dsl.file.path = '${root}/results');

USE GRAPH temporal_business;
INSERT INTO results
CALL temporal_batch() YIELD (vertex_id, state_payload, phase)
RETURN vertex_id, state_payload, phase;
