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
    creationDate varchar,
    firstName varchar,
    lastName varchar,
    gender varchar,
    browserUsed varchar,
    locationIP varchar
  ),
  Vertex Forum (
    id bigint ID,
    creationDate varchar,
    title varchar
  ),
  --Message
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

  --relations
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
	storeType='rocksdb'
);

-- Load data from SF1 dataset
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

Create Table tbl_Forum (
    creationDate varchar,
    id bigint,
    title varchar
) WITH (
  type='file',
  geaflow.dsl.file.path='${sf1_data_root}/bi_forum',
  geaflow.dsl.file.format='csv',
  `geaflow.dsl.skip.header`='true',
  geaflow.dsl.column.separator='|',
  geaflow.dsl.file.name.regex='^part-.*csv$'
);
INSERT INTO bi.Forum SELECT id, creationDate, title FROM tbl_Forum;

Create Table tbl_Post (
    creationDate varchar,
    id bigint,
    imageFile varchar,
    locationIP varchar,
    browserUsed varchar,
    `language` varchar,
    content varchar,
    length bigint
) WITH (
  type='file',
  geaflow.dsl.file.path='${sf1_data_root}/bi_post',
  geaflow.dsl.file.format='csv',
  `geaflow.dsl.skip.header`='true',
  geaflow.dsl.column.separator='|',
  geaflow.dsl.file.name.regex='^part-.*csv$'
);
INSERT INTO bi.Post
SELECT id, creationDate, browserUsed, locationIP, content, length, `language`, imageFile FROM tbl_Post;

Create Table tbl_Comment (
    creationDate varchar,
    id bigint,
    locationIP varchar,
    browserUsed varchar,
    content varchar,
    length bigint
) WITH (
  type='file',
  geaflow.dsl.file.path='${sf1_data_root}/bi_comment',
  geaflow.dsl.file.format='csv',
  `geaflow.dsl.skip.header`='true',
  geaflow.dsl.column.separator='|',
  geaflow.dsl.file.name.regex='^part-.*csv$'
);
INSERT INTO bi.Comment
SELECT id, creationDate, browserUsed, locationIP, content, length FROM tbl_Comment;

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

-- Load hasCreator edges from Comment
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

INSERT INTO bi.hasCreator
SELECT CommentId, PersonId
FROM tbl_comment_hasCreator;

-- Load hasCreator edges from Post
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

INSERT INTO bi.hasCreator
SELECT PostId, PersonId
FROM tbl_post_hasCreator;
