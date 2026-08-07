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

-- LDBC BI Graph Schema for Issue #363 (SF1 dataset)
--
-- This schema intentionally loads only the data needed by Issue #363 queries:
-- - Person vertices
-- - knows edges
-- - hasCreator edges (+ minimal Post/Comment vertices derived from edge files)
--
-- This avoids loading the full Post/Comment vertex files (which are large) and
-- keeps the benchmark focused on query optimization rather than full graph ingestion.

CREATE GRAPH bi (
  Vertex Person (
    id bigint ID,
    creationDate varchar,
    firstName varchar,
    lastName varchar,
    gender varchar,
    browserUsed varchar,
    locationIP varchar
  ),
  Vertex Post (
    id bigint ID,
    creationDate varchar,
    browserUsed varchar,
    locationIP varchar,
    content varchar,
    length bigint,
    lang varchar,
    imageFile varchar
  ),
  Vertex Comment (
    id bigint ID,
    creationDate varchar,
    browserUsed varchar,
    locationIP varchar,
    content varchar,
    length bigint
  ),

  Edge knows (
    srcId bigint SOURCE ID,
    targetId bigint DESTINATION ID,
    creationDate varchar
  ),
  Edge hasCreator (
    srcId bigint SOURCE ID,
    targetId bigint DESTINATION ID
  )
) WITH (
  storeType='rocksdb',
  shardCount=${issue363_sf1_shard_count}
);

-- Person vertices
Create Table tbl_Person (
    creationDate varchar,
    id bigint,
    firstName varchar,
    lastName varchar,
    gender varchar,
    birthday varchar,
    locationIP varchar,
    browserUsed varchar,
    `language` varchar,
    email varchar
) WITH (
  type='file',
  geaflow.dsl.file.path='${sf1_data_root}/bi_person',
  geaflow.dsl.file.format='csv',
  `geaflow.dsl.skip.header`='true',
  geaflow.dsl.column.separator='|',
  geaflow.dsl.file.name.regex='^part-.*csv$'
);

INSERT INTO bi.Person
SELECT id, creationDate, firstName, lastName, gender, browserUsed, locationIP FROM tbl_Person;

-- knows edges
Create Table tbl_knows (
    creationDate varchar,
    Person1Id bigint,
    Person2Id bigint
) WITH (
  type='file',
  geaflow.dsl.file.path = '${sf1_data_root}/bi_person_knows_person',
  geaflow.dsl.file.format='csv',
  `geaflow.dsl.skip.header`='true',
  geaflow.dsl.column.separator='|',
  geaflow.dsl.file.name.regex='^part-.*csv$'
);

INSERT INTO bi.knows
SELECT Person1Id, Person2Id, creationDate
FROM tbl_knows;

-- hasCreator edges + minimal Comment vertices (from edge files)
Create Table tbl_comment_hasCreator (
    creationDate varchar,
    CommentId bigint,
    PersonId bigint
) WITH (
  type='file',
  geaflow.dsl.file.path = '${sf1_data_root}/bi_comment_hasCreator_person',
  geaflow.dsl.file.format='csv',
  `geaflow.dsl.skip.header`='true',
  geaflow.dsl.column.separator='|',
  geaflow.dsl.file.name.regex='^part-.*csv$'
);

INSERT INTO bi.Comment
SELECT
  CommentId,
  creationDate,
  cast(null as varchar),
  cast(null as varchar),
  cast(null as varchar),
  cast(null as bigint)
FROM tbl_comment_hasCreator;

INSERT INTO bi.hasCreator
SELECT CommentId, PersonId
FROM tbl_comment_hasCreator;

-- hasCreator edges + minimal Post vertices (from edge files)
Create Table tbl_post_hasCreator (
    creationDate varchar,
    PostId bigint,
    PersonId bigint
) WITH (
  type='file',
  geaflow.dsl.file.path = '${sf1_data_root}/bi_post_hasCreator_person',
  geaflow.dsl.file.format='csv',
  `geaflow.dsl.skip.header`='true',
  geaflow.dsl.column.separator='|',
  geaflow.dsl.file.name.regex='^part-.*csv$'
);

INSERT INTO bi.Post
SELECT
  PostId,
  creationDate,
  cast(null as varchar),
  cast(null as varchar),
  cast(null as varchar),
  cast(null as bigint),
  cast(null as varchar),
  cast(null as varchar)
FROM tbl_post_hasCreator;

INSERT INTO bi.hasCreator
SELECT PostId, PersonId
FROM tbl_post_hasCreator;
