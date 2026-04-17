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

set geaflow.dsl.window.size = -1;
set geaflow.dsl.gcn.vertex.feature.fields = age;
set geaflow.dsl.gcn.batch.size = 2;
set geaflow.infer.env.enable = true;
set geaflow.infer.env.user.transform.classname = GCNBatchMarkerTransform;
set geaflow.infer.env.conda.url = '';

CREATE GRAPH gcn_batch_graph (
    Vertex person (
        id bigint ID,
        name varchar,
        age int
    ),
    Edge knows (
        srcId bigint SOURCE ID,
        targetId bigint DESTINATION ID,
        weight double
    )
) WITH (
    storeType='rocksdb',
    shardNum = 1
);

CREATE TABLE tbl_result (
    node_id int,
    prediction int,
    confidence double
) WITH (
    type='file',
    geaflow.dsl.file.path='${target}'
);

INSERT INTO gcn_batch_graph.person VALUES (1, 'alice', 18);
INSERT INTO gcn_batch_graph.person VALUES (2, 'bob', 20);
INSERT INTO gcn_batch_graph.person VALUES (3, 'cathy', 22);
INSERT INTO gcn_batch_graph.person VALUES (4, 'diana', 24);
INSERT INTO gcn_batch_graph.person VALUES (5, 'ella', 26);
INSERT INTO gcn_batch_graph.knows VALUES (1, 2, 1.0);
INSERT INTO gcn_batch_graph.knows VALUES (2, 3, 1.0);
INSERT INTO gcn_batch_graph.knows VALUES (3, 4, 1.0);
INSERT INTO gcn_batch_graph.knows VALUES (4, 5, 1.0);

USE GRAPH gcn_batch_graph;

INSERT INTO tbl_result
CALL gcn() YIELD (node_id, embedding, prediction, confidence)
RETURN cast(node_id as int), prediction, confidence
;
