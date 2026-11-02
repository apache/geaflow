set geaflow.dsl.window.size = -1;
set geaflow.dsl.ignore.exception = true;

CREATE GRAPH IF NOT EXISTS g (
  Vertex v (
    vid varchar ID,
    vvalue int
  ),
  Edge e (
    srcId varchar SOURCE ID,
    targetId varchar DESTINATION ID
  )
) WITH (
  storeType='rocksdb',
  shardCount = 1
);

CREATE TABLE IF NOT EXISTS v_source (
    v_id varchar,
    v_value int,
    ts varchar,
    type varchar
) WITH (
  type='file',
  geaflow.dsl.file.path = 'resource:///input/test_vertex'
);

CREATE TABLE IF NOT EXISTS e_source (
    src_id varchar,
    dst_id varchar
) WITH (
  type='file',
  geaflow.dsl.file.path = 'resource:///input/test_edge'
);

CREATE TABLE IF NOT EXISTS tbl_result (
  v_id varchar,
  score double
) WITH (
  type='file',
  geaflow.dsl.file.path = '${target}'
);

USE GRAPH g;

INSERT INTO g.v(vid, vvalue)
SELECT
v_id, v_value
FROM v_source;

INSERT INTO g.e(srcId, targetId)
SELECT
 src_id, dst_id
FROM e_source;

INSERT INTO tbl_result(v_id, score)
CALL betweenness_centrality(10) YIELD (id, score)
RETURN id, score;
