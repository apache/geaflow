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

package org.apache.geaflow.dsl.runtime;

import static org.apache.geaflow.common.config.keys.DSLConfigKeys.GEAFLOW_DSL_COMPILE_PHYSICAL_PLAN_ENABLE;

import com.google.common.collect.Sets;
import com.google.gson.Gson;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.commons.io.IOUtils;
import org.apache.geaflow.dsl.common.compile.CompileContext;
import org.apache.geaflow.dsl.common.compile.CompileResult;
import org.apache.geaflow.dsl.common.compile.FunctionInfo;
import org.apache.geaflow.dsl.common.compile.QueryCompiler;
import org.apache.geaflow.dsl.common.compile.TableInfo;
import org.testng.Assert;
import org.testng.annotations.Test;

public class CompilerTest {

    @Test
    public void testCompile() throws IOException {
        QueryCompiler compiler = new QueryClient();
        String script = IOUtils.resourceToString("/query/compile.sql", Charset.defaultCharset());
        CompileContext context = new CompileContext();
        Map<String, String> config = new HashMap<>();
        config.put(GEAFLOW_DSL_COMPILE_PHYSICAL_PLAN_ENABLE.getKey(), Boolean.TRUE.toString());
        context.setConfig(config);
        context.setParallelisms(new HashMap<>());

        CompileResult result = compiler.compile(script, context);
        Assert.assertEquals(result.getSourceGraphs().stream().map(g -> g.getGraphName()).collect(
            Collectors.toSet()), Sets.newHashSet("modern"));
        Assert.assertEquals(result.getTargetGraphs(), Sets.newHashSet());

        Assert.assertEquals(result.getSourceTables(), Sets.newHashSet());
        Assert.assertEquals(result.getTargetTables().stream().map(t -> t.getTableName()).collect(
            Collectors.toSet()), Sets.newHashSet("tbl_result"));
        Gson gson = new Gson();
        Assert.assertEquals(gson.toJson(result.getPhysicPlan()),
            "{\"vertices\":{\"1\":{\"vertexType\":\"source\",\"id\":\"1\",\"parallelism\":1,\"parents\":[],"
                + "\"innerPlan\":{\"vertices\":{\"1-1\":{\"id\":\"1-1\",\"parallelism\":1,"
                + "\"operator\":\"WindowSourceOperator\",\"operatorName\":\"1\",\"parents\":[]},"
                + "\"1-4\":{\"id\":\"1-4\",\"parallelism\":1,\"operator\":\"KeySelectorOperator\","
                + "\"operatorName\":\"4\",\"parents\":[{\"id\":\"1-1\"}]}}}},\"2\":{\"vertexType\":\"source\","
                + "\"id\":\"2\",\"parallelism\":1,\"parents\":[],"
                + "\"innerPlan\":{\"vertices\":{\"2-2\":{\"id\":\"2-2\",\"parallelism\":1,"
                + "\"operator\":\"WindowSourceOperator\",\"operatorName\":\"2\",\"parents\":[]},"
                + "\"2-5\":{\"id\":\"2-5\",\"parallelism\":1,\"operator\":\"KeySelectorOperator\","
                + "\"operatorName\":\"5\",\"parents\":[{\"id\":\"2-2\"}]}}}},"
                + "\"3\":{\"vertexType\":\"vertex_centric\",\"id\":\"3\",\"parallelism\":2,"
                + "\"operator\":\"StaticGraphVertexCentricTraversalAllOp\","
                + "\"operatorName\":\"GeaFlowStaticVCTraversal\",\"parents\":[{\"id\":\"3\","
                + "\"partitionType\":\"key\"},{\"id\":\"2\",\"partitionType\":\"key\"},{\"id\":\"1\","
                + "\"partitionType\":\"key\"}]},\"6\":{\"vertexType\":\"process\",\"id\":\"6\",\"parallelism\":2,"
                + "\"parents\":[{\"id\":\"3\",\"partitionType\":\"forward\"}],"
                + "\"innerPlan\":{\"vertices\":{\"6-7\":{\"id\":\"6-7\",\"parallelism\":2,\"operator\":\"MapOperator\","
                + "\"operatorName\":\"Project-1\",\"parents\":[{\"id\":\"6-6\"}]},\"6-8\":{\"id\":\"6-8\","
                + "\"parallelism\":2,\"operator\":\"SinkOperator\",\"operatorName\":\"TableSink-2\","
                + "\"parents\":[{\"id\":\"6-7\"}]},\"6-6\":{\"id\":\"6-6\",\"parallelism\":2,"
                + "\"operator\":\"FlatMapOperator\",\"operatorName\":\"TraversalResponseToRow-0\",\"parents\":[]}}}}}}"
        );
    }


    @Test
    public void testFindUnResolvedFunctions() {
        QueryCompiler compiler = new QueryClient();
        CompileContext context = new CompileContext();

        String script = "create function f0 as 'com.antgroup.udf.TestUdf';"
            + "select f1(name), substr(name, 1, 10) from t0;"
            + "use instance instance0;"
            + "select f2(id), max(id) from t0;"
            + "create view v0(c0, c1) as select f3(id) as c0, c1 from t1";

        Set<FunctionInfo> unResolvedFunctions = compiler.getUnResolvedFunctions(script, context);
        Assert.assertEquals(unResolvedFunctions.size(), 4);
        List<String> functions =
            unResolvedFunctions.stream().map(FunctionInfo::toString).collect(Collectors.toList());
        Assert.assertEquals(functions.get(0), "instance0.f3");
        Assert.assertEquals(functions.get(1), "instance0.f2");
        Assert.assertEquals(functions.get(2), "default.f1");
        Assert.assertEquals(functions.get(3), "default.f0");
    }

    @Test
    public void testFindTables() {
        QueryCompiler compiler = new QueryClient();
        CompileContext context = new CompileContext();

        String script = "insert into t1(id,name) select 1,\"tom\";\n"
            + "insert into t2 select id,name from t1;\n"
            + "insert into t4 select * from t3;";

        Set<TableInfo> tables = compiler.getUnResolvedTables(script, context);
        Assert.assertEquals(tables.size(), 4);
        Assert.assertTrue(tables.contains(new TableInfo("default", "t1")));
        Assert.assertTrue(tables.contains(new TableInfo("default", "t2")));
        Assert.assertTrue(tables.contains(new TableInfo("default", "t3")));
        Assert.assertTrue(tables.contains(new TableInfo("default", "t4")));
    }

    @Test
    public void testFindTables2() {
        QueryCompiler compiler = new QueryClient();
        CompileContext context = new CompileContext();

        String script = "CREATE GRAPH dy_modern (\n"
            + "\tVertex person (\n"
            + "\t  id bigint ID,\n"
            + "\t  name varchar,\n"
            + "\t  age int\n"
            + "\t),\n"
            + "\tVertex software (\n"
            + "\t  id bigint ID,\n"
            + "\t  name varchar,\n"
            + "\t  lang varchar\n"
            + "\t),\n"
            + "\tEdge knows (\n"
            + "\t  srcId bigint SOURCE ID,\n"
            + "\t  targetId bigint DESTINATION ID,\n"
            + "\t  weight double\n"
            + "\t),\n"
            + "\tEdge created (\n"
            + "\t  srcId bigint SOURCE ID,\n"
            + "  \ttargetId bigint DESTINATION ID,\n"
            + "  \tweight double\n"
            + "\t)\n"
            + ") WITH (\n"
            + "\tstoreType='rocksdb',\n"
            + "\tshardCount = 2\n"
            + ");\n"
            + "\n"
            + "CREATE TABLE tbl_result (\n"
            + "  a_id bigint,\n"
            + "  weight double,\n"
            + "  b_id bigint\n"
            + ") WITH (\n"
            + "\ttype='file',\n"
            + "\tgeaflow.dsl.file.path='${target}'\n"
            + ");\n"
            + "\n"
            + "USE GRAPH dy_modern;\n"
            + "\n"
            + "INSERT INTO dy_modern.person(id, name, age)\n"
            + "SELECT 1, 'jim', 20\n"
            + "UNION ALL\n"
            + "SELECT 2, 'kate', 22\n"
            + ";\n"
            + "\n"
            + "INSERT INTO dy_modern.knows\n"
            + "SELECT 1, 2, 0.2\n"
            + ";\n"
            + "\n"
            + "\n"
            + "INSERT INTO dy_modern(person.id, person.name, knows.srcId, knows.targetId)\n"
            + "SELECT 3, 'jim', 3, 2\n"
            + ";\n"
            + "\n"
            + "INSERT INTO tbl_result\n"
            + "SELECT a_id, weight, b_id\n"
            + "FROM (\n"
            + "  MATCH (a:person where id = 1) -[e:knows]->(b:person)\n"
            + "  RETURN a.id as a_id, e.weight as weight, b.id as b_id\n"
            + ");\n"
            + "\n"
            + "INSERT INTO t1 select * from t2;\n";

        Set<TableInfo> tables = compiler.getUnResolvedTables(script, context);
        Assert.assertEquals(tables.size(), 2);
        Assert.assertTrue(tables.contains(new TableInfo("default", "t1")));
        Assert.assertTrue(tables.contains(new TableInfo("default", "t2")));
        Assert.assertFalse(tables.contains(new TableInfo("default", "tbl_result")));
        Assert.assertFalse(tables.contains(new TableInfo("default", "dy_modern")));
    }

    @Test
    public void testFindTables3() {
        QueryCompiler compiler = new QueryClient();
        CompileContext context = new CompileContext();

        String script = "insert into t1(id) select * from (select name from t2);";

        Set<TableInfo> tables = compiler.getUnResolvedTables(script, context);
        Assert.assertEquals(tables.size(), 2);
        Assert.assertTrue(tables.contains(new TableInfo("default", "t1")));
        Assert.assertTrue(tables.contains(new TableInfo("default", "t2")));
    }

    @Test
    public void testFindTables4() {
        QueryCompiler compiler = new QueryClient();
        CompileContext context = new CompileContext();

        String script = "insert into t1(id) select t2.id from t2 join t3 on t2.id = t3.id;";

        Set<TableInfo> tables = compiler.getUnResolvedTables(script, context);
        Assert.assertEquals(tables.size(), 3);
        Assert.assertTrue(tables.contains(new TableInfo("default", "t1")));
        Assert.assertTrue(tables.contains(new TableInfo("default", "t2")));
        Assert.assertTrue(tables.contains(new TableInfo("default", "t3")));
    }

    @Test
    public void testFindTables5() {
        QueryCompiler compiler = new QueryClient();
        CompileContext context = new CompileContext();

        String script = "CREATE TABLE tbl_result (\n"
            + "  a_id bigint,\n"
            + "  b_id bigint,\n"
            + "  weight double\n"
            + ") WITH (\n"
            + "\ttype='file',\n"
            + "\tgeaflow.dsl.file.path='${target}'\n"
            + ");\n"
            + "\n"
            + "USE GRAPH modern;\n"
            + "\n"
            + "INSERT INTO tbl_result\n"
            + "SELECT\n"
            + "\ta_id,\n"
            + "\tb_id,\n"
            + "\tweight\n"
            + "FROM (\n"
            + "  WITH p AS (\n"
            + "    SELECT * FROM t2 AS t(id, weight)\n"
            + "  )\n"
            + "  MATCH (a:person where a.id = p.id) -[e where weight > p.weight + 0.1]->(b)\n"
            + "  RETURN a.id as a_id, e.weight as weight, b.id as b_id\n"
            + ")";

        Set<TableInfo> tables = compiler.getUnResolvedTables(script, context);
        Assert.assertEquals(tables.size(), 2);
        Assert.assertTrue(tables.contains(new TableInfo("default", "p")));
        Assert.assertTrue(tables.contains(new TableInfo("default", "t2")));
        Assert.assertFalse(tables.contains(new TableInfo("default", "tbl_result")));
    }
}
