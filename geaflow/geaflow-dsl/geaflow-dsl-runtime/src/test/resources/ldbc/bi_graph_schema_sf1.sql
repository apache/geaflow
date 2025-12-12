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

-- LDBC BI Graph Schema with SF1 Dataset (660x scale)
-- 9,892 Person vertices, 180,623 knows edges
-- 2.05M Comments, 1.00M Posts, 90K Forums

CREATE GRAPH bi (
  --dynamic
  Vertex Person (
    id bigint ID,
    creationDate bigint,
    firstName varchar,
    lastName varchar,
    gender varchar,
    browserUsed varchar,
    locationIP varchar
  ),
  Vertex Forum (
    id bigint ID,
    creationDate bigint,
    title varchar
  ),
  --Message
  Vertex Post (
    id bigint ID,
    creationDate bigint,
    browserUsed varchar,
    locationIP varchar,
    content varchar,
    length bigint,
    lang varchar,
    imageFile varchar
  ),
  Vertex Comment (
    id bigint ID,
    creationDate bigint,
    browserUsed varchar,
    locationIP varchar,
    content varchar,
    length bigint
  ),

  --relations
  Edge knows (
    srcId bigint SOURCE ID,
    targetId bigint DESTINATION ID,
    creationDate bigint
  ),
  Edge hasCreator (
    srcId bigint SOURCE ID,
    targetId bigint DESTINATION ID
  )
) WITH (
	storeType='rocksdb'
);

-- Load data from SF1 dataset
Create Table tbl_Person (id bigint, type varchar, creationDate bigint, firstName varchar,
    lastName varchar, gender varchar, browserUsed varchar, locationIP varchar)
WITH ( type='file', geaflow.dsl.file.path='resource:///data_sf1/bi_person');
INSERT INTO bi.Person
SELECT id, creationDate, firstName, lastName, gender, browserUsed, locationIP FROM tbl_Person;

Create Table tbl_Forum (id bigint, type varchar, creationDate bigint, title varchar)
WITH ( type='file', geaflow.dsl.file.path='resource:///data_sf1/bi_forum');
INSERT INTO bi.Forum SELECT id, creationDate, title FROM tbl_Forum;

Create Table tbl_Post (id bigint, type varchar, creationDate bigint, browserUsed varchar,
    locationIP varchar, content varchar, length bigint, lang varchar, imageFile varchar)
WITH ( type='file', geaflow.dsl.file.path='resource:///data_sf1/bi_post');
INSERT INTO bi.Post
SELECT id, creationDate, browserUsed, locationIP, content, length, lang, imageFile FROM tbl_Post;

Create Table tbl_Comment (id bigint, type varchar, creationDate bigint, browserUsed varchar,
    locationIP varchar, content varchar, length bigint)
WITH ( type='file', geaflow.dsl.file.path='resource:///data_sf1/bi_comment');
INSERT INTO bi.Comment
SELECT id, creationDate, browserUsed, locationIP, content, length FROM tbl_Comment;

Create Table tbl_knows (
    person1Id bigint,
    person2Id bigint,
    creationDate bigint
) WITH (
  type='file',
  geaflow.dsl.file.path = 'resource:///data_sf1/bi_person_knows_person'
);

INSERT INTO bi.knows
SELECT person1Id, person2Id, creationDate
FROM tbl_knows;

-- Load hasCreator edges from Comment
Create Table tbl_comment_hasCreator (
    commentId bigint,
    personId bigint
) WITH (
  type='file',
  geaflow.dsl.file.path = 'resource:///data_sf1/bi_comment_hasCreator_person'
);

INSERT INTO bi.hasCreator
SELECT commentId, personId
FROM tbl_comment_hasCreator;

-- Load hasCreator edges from Post
Create Table tbl_post_hasCreator (
    postId bigint,
    personId bigint
) WITH (
  type='file',
  geaflow.dsl.file.path = 'resource:///data_sf1/bi_post_hasCreator_person'
);

INSERT INTO bi.hasCreator
SELECT postId, personId
FROM tbl_post_hasCreator;
