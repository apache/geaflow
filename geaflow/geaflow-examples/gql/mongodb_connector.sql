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

CREATE TABLE mongo_source (
  id BIGINT,
  name VARCHAR,
  active BOOLEAN
) WITH (
  type = 'mongodb',
  `geaflow.dsl.mongodb.uri` = 'mongodb://localhost:27017',
  `geaflow.dsl.mongodb.database` = 'geaflow',
  `geaflow.dsl.mongodb.collection` = 'source_records',
  `geaflow.dsl.mongodb.partition.num` = '4',
  `geaflow.dsl.mongodb.partition.field` = 'id',
  `geaflow.dsl.mongodb.partition.lowerbound` = '0',
  `geaflow.dsl.mongodb.partition.upperbound` = '100'
);

CREATE TABLE mongo_sink (
  id BIGINT,
  name VARCHAR,
  active BOOLEAN
) WITH (
  type = 'mongodb',
  `geaflow.dsl.mongodb.uri` = 'mongodb://localhost:27017',
  `geaflow.dsl.mongodb.database` = 'geaflow',
  `geaflow.dsl.mongodb.collection` = 'sink_records',
  `geaflow.dsl.mongodb.batch.size` = '500'
);

INSERT INTO mongo_sink
SELECT id, name, active FROM mongo_source;
