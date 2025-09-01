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

package org.apache.geaflow.dsl;

import org.apache.geaflow.dsl.optimize.rule.FilterMatchNodeTransposeRule;
import org.apache.geaflow.dsl.optimize.rule.FilterToMatchRule;
import org.apache.geaflow.dsl.optimize.rule.MatchEdgeLabelFilterRemoveRule;
import org.apache.geaflow.dsl.optimize.rule.MatchIdFilterSimplifyRule;
import org.apache.geaflow.dsl.optimize.rule.TableScanToGraphRule;
import org.testng.annotations.Test;

public class GQLToRelConverterTest {

    private static final String GRAPH_G1 = "create graph g1("
        + "vertex user("
        + " id bigint ID,"
        + "name varchar"
        + "),"
        + "vertex person("
        + " id bigint ID,"
        + "name varchar,"
        + "gender int,"
        + "age integer"
        + "),"
        + "edge knows("
        + " src_id bigint SOURCE ID,"
        + " target_id bigint DESTINATION ID,"
        + " time bigint TIMESTAMP,"
        + " weight double"
        + ")"
        + ")";

    @Test
    public void testMatchPattern() {
        PlanTester.build()
            .gql("match(a:user)-[e:knows]->(b:user)")
            .toRel()
            .checkRelNode(
                "LogicalGraphMatch(path=[(a:user)-[e:knows]->(b:user)])\n"
                    + "  LogicalGraphScan(table=[default.g0])\n"
            );
    }

    @Test
    public void testMatchPattern2() {
        PlanTester.build()
            .gql("MATCH (a:user)-[e:knows]->(b)->(c)-[]->(d)<-[e2]-(f)")
            .toRel()
            .checkRelNode(
                "LogicalGraphMatch(path=[(a:user)-[e:knows]->(b:)-[e_col_1:]->(c:)-[e_col_2:]->(d:)-[e2:]<-(f:)])\n"
                    + "  LogicalGraphScan(table=[default.g0])\n"
            );
    }

    @Test
    public void testReturn() {
        PlanTester.build()
            .gql("match(a:user)-[e:knows]->(b:user)\n"
                + "RETURN a.id AS a_id, e, b.id")
            .toRel()
            .checkRelNode(
                "LogicalProject(a_id=[$0.id], e=[$1], id=[$2.id])\n"
                    + "  LogicalGraphMatch(path=[(a:user)-[e:knows]->(b:user)])\n"
                    + "    LogicalGraphScan(table=[default.g0])\n"
            );
    }

    @Test
    public void testReturn2() {
        String graph = "create graph g1("
            + "vertex user("
            + " id bigint ID,"
            + "name varchar"
            + "),"
            + "vertex person("
            + " id bigint ID,"
            + "name varchar,"
            + "gender int,"
            + "age integer"
            + "),"
            + "edge knows("
            + " src_id bigint SOURCE ID,"
            + " target_id bigint DESTINATION ID,"
            + " time bigint TIMESTAMP,"
            + " weight double"
            + "),"
            + "edge follow("
            + " src_id bigint SOURCE ID,"
            + " target_id bigint DESTINATION ID,"
            + " time bigint TIMESTAMP,"
            + " weight double"
            + ")"
            + ")";
        PlanTester.build()
            .registerGraph(graph)
            .gql("MATCH (a:user|person WHERE id = 1)-[e:knows|follow]->(b:user)\n"
                + "RETURN a, e, b")
            .toRel()
            .checkRelNode(
                "LogicalProject(a=[$0], e=[$1], b=[$2])\n"
                    + "  LogicalGraphMatch(path=[(a:user|person) where =(a.id, 1) "
                    + "-[e:knows|follow]->(b:user)])\n"
                    + "    LogicalGraphScan(table=[default.g1])\n"
            );
    }

    @Test
    public void testVertexScan() {
        PlanTester.build()
            .gql("select id from user")
            .toRel()
            .checkRelNode(
                "LogicalProject(id=[$0])\n"
                    + "  LogicalTableScan(table=[[default, user]])\n"
            )
            .opt(new TableScanToGraphRule())
            .checkRelNode(
                "LogicalProject(id=[$0])\n"
                    + "  LogicalProject(f0=[user.id], f1=[user.~label], f2=[user.name], f3=[user"
                    + ".age])\n"
                    + "    LogicalGraphMatch(path=[(user:user)])\n"
                    + "      LogicalGraphScan(table=[null])\n"
            );
    }

    @Test
    public void testVertexIdFilterSimplify() {
        PlanTester.build()
            .gql("MATCH (a:user where id = 1)-[e:knows]-(b:user)\n"
                + "RETURN a.id as a_id, e.weight as weight, b.id as b_id")
            .toRel()
            .checkRelNode(
                "LogicalProject(a_id=[$0.id], weight=[$1.weight], b_id=[$2.id])\n"
                    + "  LogicalGraphMatch(path=[(a:user) where =(a.id, 1) -[e:knows]-(b:user)])\n"
                    + "    LogicalGraphScan(table=[default.g0])\n"
            )
            .opt(MatchIdFilterSimplifyRule.INSTANCE)
            .checkRelNode(
                "LogicalProject(a_id=[$0.id], weight=[$1.weight], b_id=[$2.id])\n"
                    + "  LogicalGraphMatch(path=[(a:user)-[e:knows]-(b:user)])\n"
                    + "    LogicalGraphScan(table=[default.g0])\n"
            );
    }

    @Test
    public void testMatchEdgeLabelRemove() {
        PlanTester.build()
            .gql("MATCH (a:user where id = 1)-[e:knows]-(b:user) WHERE e.~label = 'knows' "
                + "or e.~label = 'created'\n"
                + "RETURN a.id as a_id, e.weight as weight, b.id as b_id")
            .toRel()
            .checkRelNode(
                "LogicalProject(a_id=[$0.id], weight=[$1.weight], b_id=[$2.id])\n"
                    + "  LogicalGraphMatch(path=[(a:user) where =(a.id, 1) -[e:knows]-(b:user) "
                    + "where OR(=($1.~label, _UTF-16LE'knows'), =($1.~label, _UTF-16LE'created'))"
                    + " ])\n"
                    + "    LogicalGraphScan(table=[default.g0])\n"
            )
            .opt(MatchEdgeLabelFilterRemoveRule.INSTANCE, FilterMatchNodeTransposeRule.INSTANCE, FilterToMatchRule.INSTANCE)
            .checkRelNode(
                "LogicalProject(a_id=[$0.id], weight=[$1.weight], b_id=[$2.id])\n"
                    + "  LogicalGraphMatch(path=[(a:user) where =(a.id, 1) -[e:knows]-(b:user)])\n"
                    + "    LogicalGraphScan(table=[default.g0])\n"
            );
    }

    @Test
    public void testFilter() {
        PlanTester.build()
            .gql("match(a:user)-[e:knows]->(b:user)\n"
                + "RETURN a.id AS a_id, e, b.id\n"
                + "THEN FILTER a_id = 1 AND e.src_id = 1")
            .toRel()
            .checkRelNode(
                "LogicalFilter(condition=[AND(=($0, 1), =($1.src_id, 1))])\n"
                    + "  LogicalProject(a_id=[$0.id], e=[$1], id=[$2.id])\n"
                    + "    LogicalGraphMatch(path=[(a:user)-[e:knows]->(b:user)])\n"
                    + "      LogicalGraphScan(table=[default.g0])\n"
            );
    }

    @Test
    public void testFilter2() {
        PlanTester.build()
            .gql("match(a:user)-[e:knows]->(b:user)\n"
                + "RETURN a.id as a_id, e, b.id\n"
                + "THEN FILTER a_id = 1 OR id = 1 AND CAST(e.weight as int) = 1")
            .toRel()
            .checkRelNode(
                "LogicalFilter(condition=[OR(=($0, 1), AND(=($2, 1), =(CAST($1.weight):INTEGER, 1)))])\n"
                    + "  LogicalProject(a_id=[$0.id], e=[$1], id=[$2.id])\n"
                    + "    LogicalGraphMatch(path=[(a:user)-[e:knows]->(b:user)])\n"
                    + "      LogicalGraphScan(table=[default.g0])\n"
            );
    }

    @Test
    public void testReturnOrderBy1() {
        PlanTester
            .build()
            .registerGraph(GRAPH_G1)
            .gql("MATCH (a:person WHERE a.id = '1')-[e:knows]->(b:person) "
                + "RETURN a.name as name, b.id as _id, b.age as age order by name DESC")
            .toRel()
            .checkRelNode(
                "LogicalSort(sort0=[$0], dir0=[DESC])\n"
                    + "  LogicalProject(name=[$0.name], _id=[$2.id], age=[$2.age])\n"
                    + "    LogicalGraphMatch(path=[(a:person) where =(a.id, _UTF-16LE'1') "
                    + "-[e:knows]->(b:person)])\n"
                    + "      LogicalGraphScan(table=[default.g1])\n"
            );
    }

    @Test
    public void testReturnOrderBy2() {
        PlanTester
            .build()
            .registerGraph(GRAPH_G1)
            .gql("MATCH (a:person WHERE a.id = '1')-[e:knows]->(b:person) "
                + "RETURN b.name as name, b.id as _id, a.age as a_age, b.age as b_age order by"
                + " b_age + 10 DESC Limit 10")
            .toRel()
            .checkRelNode(
                "LogicalProject(name=[$0], _id=[$1], a_age=[$2], b_age=[$3])\n"
                    + "  LogicalSort(sort0=[$4], dir0=[DESC], fetch=[10])\n"
                    + "    LogicalProject(name=[$2.name], _id=[$2.id], a_age=[$0.age], b_age=[$2"
                    + ".age], EXPR$4=[+($2.age, 10)])\n"
                    + "      LogicalGraphMatch(path=[(a:person) where =(a.id, _UTF-16LE'1') "
                    + "-[e:knows]->(b:person)])\n"
                    + "        LogicalGraphScan(table=[default.g1])\n"
            );
    }

    @Test
    public void testReturnOrderBy3() {
        PlanTester
            .build()
            .registerGraph(GRAPH_G1)
            .gql("MATCH (a:user WHERE a.id = 1)-[e:knows]->(b:user)\n"
                + "RETURN a as _a, b.id as b_id, weight Order by _a.id * 10 DESC Limit 10")
            .toRel()
            .checkRelNode(
                "LogicalProject(_a=[$0], b_id=[$1], weight=[$2])\n"
                    + "  LogicalSort(sort0=[$3], dir0=[DESC], fetch=[10])\n"
                    + "    LogicalProject(_a=[$0], b_id=[$2.id], weight=[$1.weight], EXPR$3=[*($0"
                    + ".id, 10)])\n"
                    + "      LogicalGraphMatch(path=[(a:user) where =(a.id, 1) -[e:knows]->"
                    + "(b:user)])\n"
                    + "        LogicalGraphScan(table=[default.g1])\n"
            );
    }

    @Test
    public void testReturnOrderBy4() {
        PlanTester
            .build()
            .registerGraph(GRAPH_G1)
            .gql("MATCH (a:user WHERE a.id = 1)-[e:knows]->(b:user)\n"
                + "RETURN a as _a, b.id as b_id, weight Order by a.id DESC Limit 10")
            .toRel()
            .checkRelNode(
                "LogicalProject(_a=[$0], b_id=[$1], weight=[$2])\n"
                    + "  LogicalSort(sort0=[$3], dir0=[DESC], fetch=[10])\n"
                    + "    LogicalProject(_a=[$0], b_id=[$2.id], weight=[$1.weight], id=[$0.id])\n"
                    + "      LogicalGraphMatch(path=[(a:user) where =(a.id, 1) -[e:knows]->"
                    + "(b:user)])\n"
                    + "        LogicalGraphScan(table=[default.g1])\n"
            );
    }

    @Test
    public void testReturnStmt1() {
        PlanTester
            .build()
            .registerGraph(GRAPH_G1)
            .gql("MATCH (a:user where a.id = 1)-[e:knows where e.weight > 0.4]->(b:user) RETURN a")
            .toRel()
            .checkRelNode(
                "LogicalProject(a=[$0])\n"
                    + "  LogicalGraphMatch(path=[(a:user) where =(a.id, 1) -[e:knows]-> where >"
                    + "(e.weight, 0.4) (b:user)])\n"
                    + "    LogicalGraphScan(table=[default.g1])\n"
            );
    }

    @Test
    public void testReturnStmt2() {
        PlanTester
            .build()
            .registerGraph(GRAPH_G1)
            .gql("Match (a:user WHERE name = 'marko')<-[e:knows]-(b:person) WHERE a.name <> b.name RETURN e")
            .toRel()
            .checkRelNode(
                "LogicalProject(e=[$1])\n"
                    + "  LogicalGraphMatch(path=[(a:user) where =(a.name, _UTF-16LE'marko') -[e:knows]<-(b:person) "
                    + "where <>($0.name, $2.name) ])\n"
                    + "    LogicalGraphScan(table=[default.g1])\n"
            );
    }

    @Test
    public void testReturnStmt3() {
        PlanTester
            .build()
            .registerGraph(GRAPH_G1)
            .gql("MATCH (a:person WHERE a.age > 18) - (b: person) RETURN b")
            .toRel()
            .checkRelNode(
                "LogicalProject(b=[$2])\n"
                    + "  LogicalGraphMatch(path=[(a:person) where >(a.age, 18) -[e_col_1:]-(b:person)])\n"
                    + "    LogicalGraphScan(table=[default.g1])\n"
            );
    }

    @Test
    public void testReturnStmt4() {
        PlanTester
            .build()
            .registerGraph(GRAPH_G1)
            .gql("MATCH (a:person WHERE a.id = '1')-[e:knows]->(b:person) RETURN a.name as name, b.id as b_id")
            .toRel()
            .checkRelNode(
                "LogicalProject(name=[$0.name], b_id=[$2.id])\n"
                    + "  LogicalGraphMatch(path=[(a:person) where =(a.id, _UTF-16LE'1') "
                    + "-[e:knows]->(b:person)])\n"
                    + "    LogicalGraphScan(table=[default.g1])\n"
            );
    }

    @Test
    public void testReturnStmt5() {
        PlanTester
            .build()
            .registerGraph(GRAPH_G1)
            .gql("MATCH (a:person WHERE a.id = '1')-[e:knows]->(b:person) RETURN a.name as name, b.id as b_id, b.age * 10 as amt")
            .toRel()
            .checkRelNode(
                "LogicalProject(name=[$0.name], b_id=[$2.id], amt=[*($2.age, 10)])\n"
                    + "  LogicalGraphMatch(path=[(a:person) where =(a.id, _UTF-16LE'1') "
                    + "-[e:knows]->(b:person)])\n"
                    + "    LogicalGraphScan(table=[default.g1])\n"
            );
    }

    @Test
    public void testReturnStmt6() {
        PlanTester
            .build()
            .registerGraph(GRAPH_G1)
            .gql("MATCH (a:person WHERE a.id = '1')-[e:knows]->(b:person) RETURN b.name as name, cast(b.id as int) as _id")
            .toRel()
            .checkRelNode(
                "LogicalProject(name=[$2.name], _id=[CAST($2.id):INTEGER NOT NULL])\n"
                    + "  LogicalGraphMatch(path=[(a:person) where =(a.id, _UTF-16LE'1') -[e:knows]->(b:person)])\n"
                    + "    LogicalGraphScan(table=[default.g1])\n"
            );
    }

    @Test
    public void testReturnStmt7() {
        PlanTester
            .build()
            .registerGraph(GRAPH_G1)
            .gql("MATCH (a:person WHERE id = '1')-[e:knows]->(b:person) "
                + "RETURN b.name as name, case when b.gender = 0 then '0' else '1' end as _id")
            .toRel()
            .checkRelNode(
                "LogicalProject(name=[$2.name], _id=[CASE(=($2.gender, 0), _UTF-16LE'0', "
                    + "_UTF-16LE'1')])\n"
                    + "  LogicalGraphMatch(path=[(a:person) where =(a.id, _UTF-16LE'1') "
                    + "-[e:knows]->(b:person)])\n"
                    + "    LogicalGraphScan(table=[default.g1])\n"
            );
    }

    @Test
    public void testReturnStmt8() {
        PlanTester
            .build()
            .registerGraph(GRAPH_G1)
            .gql("Match (a1:user WHERE name like 'marko')-[e1]->(b1:person) return b1.name AS b_id")
            .toRel()
            .checkRelNode(
                "LogicalProject(b_id=[$2.name])\n"
                    + "  LogicalGraphMatch(path=[(a1:user) where LIKE(a1.name, _UTF-16LE'marko')"
                    + " -[e1:]->(b1:person)])\n"
                    + "    LogicalGraphScan(table=[default.g1])\n"
            );
    }

    @Test
    public void testReturnStmt9() {
        PlanTester
            .build()
            .registerGraph(GRAPH_G1)
            .gql("Match (a:user)-[e]->(b:person WHERE name = 'lop') return a.id")
            .toRel()
            .checkRelNode(
                "LogicalProject(id=[$0.id])\n"
                    + "  LogicalGraphMatch(path=[(a:user)-[e:]->(b:person) where =(b.name, "
                    + "_UTF-16LE'lop') ])\n"
                    + "    LogicalGraphScan(table=[default.g1])\n"
            );
    }

    @Test
    public void testReturnOrderBy5() {
        PlanTester
            .build()
            .registerGraph(GRAPH_G1)
            .gql("MATCH (a:user WHERE a.id = 1)-[e:knows]->(b:user)\n"
                + "RETURN a as _a, b.id as b_id, weight Order by weight DESC Limit 10")
            .toRel()
            .checkRelNode(
                "LogicalSort(sort0=[$2], dir0=[DESC], fetch=[10])\n"
                    + "  LogicalProject(_a=[$0], b_id=[$2.id], weight=[$1.weight])\n"
                    + "    LogicalGraphMatch(path=[(a:user) where =(a.id, 1) -[e:knows]->"
                    + "(b:user)])\n"
                    + "      LogicalGraphScan(table=[default.g1])\n"
            );
    }

    @Test
    public void testReturnOrderBy6() {
        PlanTester
            .build()
            .registerGraph(GRAPH_G1)
            .gql("MATCH (a:user WHERE a.id = 1)-[e:knows]->(b:user)\n"
                + "RETURN a as _a, e as _e, b.id as b_id Order by e DESC, a Limit 10")
            .toRel()
            .checkRelNode(
                "LogicalSort(sort0=[$1], sort1=[$0], dir0=[DESC], dir1=[ASC], fetch=[10])\n"
                    + "  LogicalProject(_a=[$0], _e=[$1], b_id=[$2.id])\n"
                    + "    LogicalGraphMatch(path=[(a:user) where =(a.id, 1) -[e:knows]->"
                    + "(b:user)])\n"
                    + "      LogicalGraphScan(table=[default.g1])\n"
            );
    }

    @Test
    public void testReturnOrderBy7() {
        PlanTester
            .build()
            .registerGraph(GRAPH_G1)
            .gql("MATCH (a:user WHERE a.id = 1)-[e:knows]->(b:user|person)\n"
                + "RETURN a as _a, e.time as e_time, gender Order by "
                + "case when b.gender > 25 then '1' else '0' end  DESC "
                + "Limit 10")
            .toRel()
            .checkRelNode(
                "LogicalProject(_a=[$0], e_time=[$1], gender=[$2])\n"
                    + "  LogicalSort(sort0=[$3], dir0=[DESC], fetch=[10])\n"
                    + "    LogicalProject(_a=[$0], e_time=[$1.time], gender=[$2.gender], "
                    + "EXPR$3=[CASE(>($2.gender, 25), _UTF-16LE'1', _UTF-16LE'0')])\n"
                    + "      LogicalGraphMatch(path=[(a:user) where =(a.id, 1) -[e:knows]->"
                    + "(b:user|person)])\n"
                    + "        LogicalGraphScan(table=[default.g1])\n"
            );
    }

    @Test
    public void testReturnOrderBy8() {
        PlanTester
            .build()
            .registerGraph(GRAPH_G1)
            .gql("MATCH (a:user WHERE a.id = 1)-[e:knows]->(b:user|person)\n"
                + "RETURN a as _a, e.time as e_time, gender Order by "
                + "case when gender > 25 then '1' else '0' end DESC "
                + "Limit 10")
            .toRel()
            .checkRelNode(
                "LogicalProject(_a=[$0], e_time=[$1], gender=[$2])\n"
                    + "  LogicalSort(sort0=[$3], dir0=[DESC], fetch=[10])\n"
                    + "    LogicalProject(_a=[$0], e_time=[$1.time], gender=[$2.gender], "
                    + "EXPR$3=[CASE(>($2.gender, 25), _UTF-16LE'1', _UTF-16LE'0')])\n"
                    + "      LogicalGraphMatch(path=[(a:user) where =(a.id, 1) -[e:knows]->"
                    + "(b:user|person)])\n"
                    + "        LogicalGraphScan(table=[default.g1])\n"
            );
    }

    @Test
    public void testReturnOrderBy9() {
        PlanTester
            .build()
            .registerGraph(GRAPH_G1)
            .gql("MATCH (a:user WHERE a.id = 1)-[e:knows]->(b:user|person)\n"
                + "RETURN a as _a, e as _e, gender Order by "
                + "a DESC, e DESC "
                + "Limit 10")
            .toRel()
            .checkRelNode(
                "LogicalSort(sort0=[$0], sort1=[$1], dir0=[DESC], dir1=[DESC], fetch=[10])\n"
                    + "  LogicalProject(_a=[$0], _e=[$1], gender=[$2.gender])\n"
                    + "    LogicalGraphMatch(path=[(a:user) where =(a.id, 1) -[e:knows]->"
                    + "(b:user|person)])\n"
                    + "      LogicalGraphScan(table=[default.g1])\n"
            );
    }

    @Test
    public void testReturnOrderBy10() {
        PlanTester
            .build()
            .registerGraph(GRAPH_G1)
            .gql("MATCH (a:user WHERE a.id = 1)-[e:knows]->(b:user|person)\n"
                + "RETURN a as _a, e as _e, gender Order by "
                + "_e DESC, _a DESC "
                + "Limit 10")
            .toRel()
            .checkRelNode(
                "LogicalSort(sort0=[$1], sort1=[$0], dir0=[DESC], dir1=[DESC], fetch=[10])\n"
                    + "  LogicalProject(_a=[$0], _e=[$1], gender=[$2.gender])\n"
                    + "    LogicalGraphMatch(path=[(a:user) where =(a.id, 1) -[e:knows]->"
                    + "(b:user|person)])\n"
                    + "      LogicalGraphScan(table=[default.g1])\n"
            );
    }

    @Test
    public void testReturnOrderBy11() {
        PlanTester
            .build()
            .registerGraph(GRAPH_G1)
            .gql("MATCH (a:user WHERE a.id = 1)-[e:knows]->(b:user|person)\n"
                + "RETURN a as _a, e as _e, gender Order by "
                + "_a.id DESC, _e.time + 1000"
                + "Limit 10")
            .toRel()
            .checkRelNode(
                "LogicalProject(_a=[$0], _e=[$1], gender=[$2])\n"
                    + "  LogicalSort(sort0=[$3], sort1=[$4], dir0=[DESC], dir1=[ASC], fetch=[10])\n"
                    + "    LogicalProject(_a=[$0], _e=[$1], gender=[$2.gender], id=[$0.id], "
                    + "EXPR$4=[+($1.time, 1000)])\n"
                    + "      LogicalGraphMatch(path=[(a:user) where =(a.id, 1) -[e:knows]->"
                    + "(b:user|person)])\n"
                    + "        LogicalGraphScan(table=[default.g1])\n"
            );
    }

    @Test
    public void testReturnGroupBy1() {
        PlanTester
            .build()
            .registerGraph(GRAPH_G1)
            .gql("MATCH (a:person)-[e:knows where e.weight > 0.4]->(b:person) "
                + "RETURN a.id as a_id, SUM(e.weight * 10) as amt GROUP BY a_id ORDER BY a_id")
            .toRel()
            .checkRelNode(
                "LogicalSort(sort0=[$0], dir0=[ASC])\n"
                    + "  LogicalAggregate(group=[{0}], amt=[SUM($1)])\n"
                    + "    LogicalProject(a_id=[$0.id], $f1=[*($1.weight, 10)])\n"
                    + "      LogicalGraphMatch(path=[(a:person)-[e:knows]-> where >(e.weight, 0"
                    + ".4) (b:person)])\n"
                    + "        LogicalGraphScan(table=[default.g1])\n"
            );
    }

    @Test
    public void testReturnGroupBy2() {
        PlanTester
            .build()
            .registerGraph(GRAPH_G1)
            .gql("MATCH (a:person)-[e:knows]->(b:person) "
                + "RETURN a as _a, e as _e, b as _b group by _a, _e, _b")
            .toRel()
            .checkRelNode(
                "LogicalAggregate(group=[{0, 1, 2}])\n"
                    + "  LogicalProject(_a=[$0], _e=[$1], _b=[$2])\n"
                    + "    LogicalGraphMatch(path=[(a:person)-[e:knows]->(b:person)])\n"
                    + "      LogicalGraphScan(table=[default.g1])\n"
            );
    }

    @Test
    public void testReturnGroupBy3() {
        PlanTester
            .build()
            .registerGraph(GRAPH_G1)
            .gql("MATCH (a:person)-[e:knows]->(b:person) "
                + "RETURN COUNT(a.name) as a_name, SUM(e.weight) as e_weight, "
                + "MAX(b.age) as b_age_max, MIN(b.age) as b_age_min, "
                + "AVG(b.age) as b_age_avg, b as _b "
                + "group by _b order by _b")
            .toRel()
            .checkRelNode(
                "LogicalSort(sort0=[$5], dir0=[ASC])\n"
                    + "  LogicalProject(a_name=[$1], e_weight=[$2], b_age_max=[$3], "
                    + "b_age_min=[$4], b_age_avg=[$5], _b=[$0])\n"
                    + "    LogicalAggregate(group=[{0}], a_name=[COUNT($1)], e_weight=[SUM($2)], "
                    + "b_age_max=[MAX($3)], b_age_min=[MIN($3)], b_age_avg=[AVG($3)])\n"
                    + "      LogicalProject(_b=[$2], $f1=[$0.name], $f2=[$1.weight], $f3=[$2"
                    + ".age])\n"
                    + "        LogicalGraphMatch(path=[(a:person)-[e:knows]->(b:person)])\n"
                    + "          LogicalGraphScan(table=[default.g1])\n"
            );
    }

    @Test
    public void testReturnGroupBy4() {
        PlanTester
            .build()
            .registerGraph(GRAPH_G1)
            .gql("MATCH (a:person)-[e:knows]->(b:person) "
                + "RETURN a as _a, e as _e, b.id as b_id group by b.id, e, a")
            .toRel()
            .checkRelNode(
                "LogicalProject(_a=[$2], _e=[$1], b_id=[$0])\n"
                    + "  LogicalAggregate(group=[{0, 1, 2}])\n"
                    + "    LogicalProject(b_id=[$2.id], _e=[$1], _a=[$0])\n"
                    + "      LogicalGraphMatch(path=[(a:person)-[e:knows]->(b:person)])\n"
                    + "        LogicalGraphScan(table=[default.g1])\n"
            );
    }

    @Test
    public void testReturnGroupBy5() {
        PlanTester
            .build()
            .registerGraph(GRAPH_G1)
            .gql("MATCH (a:user)-[e:knows]->(b:person) "
                + "RETURN `time` as e_time, age group by `time`, age")
            .toRel()
            .checkRelNode(
                "LogicalAggregate(group=[{0, 1}])\n"
                    + "  LogicalProject(e_time=[$1.time], age=[$2.age])\n"
                    + "    LogicalGraphMatch(path=[(a:user)-[e:knows]->(b:person)])\n"
                    + "      LogicalGraphScan(table=[default.g1])\n"
            );
    }

    @Test
    public void testAggregateSum() {
        String script = "MATCH (a:person)-[e:knows]->(b:person) "
            + "RETURN a.id as a_id, b.id as b_id, SUM(ALL e.weight) as e_sum GROUP BY a_id, b_id";
        PlanTester
            .build()
            .registerGraph(GRAPH_G1)
            .gql(script)
            .toRel()
            .checkRelNode(
                "LogicalAggregate(group=[{0, 1}], e_sum=[SUM($2)])\n"
                    + "  LogicalProject(a_id=[$0.id], b_id=[$2.id], $f2=[$1.weight])\n"
                    + "    LogicalGraphMatch(path=[(a:person)-[e:knows]->(b:person)])\n"
                    + "      LogicalGraphScan(table=[default.g1])\n"
            );
    }

    @Test
    public void testAggregateSum2() {
        String script = "MATCH (a:person)-[e:knows]->(b:person) "
            + "RETURN b.name as name, SUM(ALL b.age) as amt group by name order by name";
        PlanTester
            .build()
            .registerGraph(GRAPH_G1)
            .gql(script)
            .toRel()
            .checkRelNode(
                "LogicalSort(sort0=[$0], dir0=[ASC])\n"
                    + "  LogicalAggregate(group=[{0}], amt=[SUM($1)])\n"
                    + "    LogicalProject(name=[$2.name], $f1=[$2.age])\n"
                    + "      LogicalGraphMatch(path=[(a:person)-[e:knows]->(b:person)])\n"
                    + "        LogicalGraphScan(table=[default.g1])\n"
            );
    }

    @Test
    public void testAggregateSum3() {
        String script = "MATCH (a:person)-[e:knows where e.weight > 0.4]->(b:person) "
            + "RETURN a.id, SUM(ALL e.weight) * 10 as amt GROUP BY a.id order by a.id";
        PlanTester
            .build()
            .registerGraph(GRAPH_G1)
            .gql(script)
            .toRel()
            .checkRelNode(
                "LogicalSort(sort0=[$0], dir0=[ASC])\n"
                    + "  LogicalProject(id=[$0], amt=[*($1, 10)])\n"
                    + "    LogicalAggregate(group=[{0}], agg#0=[SUM($1)])\n"
                    + "      LogicalProject(id=[$0.id], $f1=[$1.weight])\n"
                    + "        LogicalGraphMatch(path=[(a:person)-[e:knows]-> where >(e.weight, 0.4) (b:person)])\n"
                    + "          LogicalGraphScan(table=[default.g1])\n"
            );
    }

    @Test
    public void testGQLLet1() {
        String script = "MATCH(a:person)\n"
            + " Let a.weight = a.age / 10\n"
            + " Let a.weight = a.weight * 2\n"
            + " Let a.ratio = 1.0";

        PlanTester
            .build()
            .registerGraph(GRAPH_G1)
            .gql(script)
            .toRel()
            .checkRelNode(
                "LogicalGraphMatch(path=[(a:person) PathModify([a=VERTEX{id:$0.id, ~label:$0.~label, "
                    + "name:$0.name, gender:$0.gender, age:$0.age, weight:CAST(/($0.age, 10)):JavaType(class java.lang.Integer)}]) "
                    + "PathModify([a=VERTEX{id:$0.id, ~label:$0.~label, name:$0.name, gender:$0.gender, age:$0.age, "
                    + "weight:CAST(*($0.weight, 2)):JavaType(class java.lang.Integer)}]) PathModify([a=VERTEX{id:$0"
                    + ".id, ~label:$0.~label, name:$0.name, gender:$0.gender, age:$0.age, weight:$0.weight, ratio:1"
                    + ".0}])])\n"
                    + "  LogicalGraphScan(table=[default.g1])\n");
    }

    @Test
    public void testGQLLet2() {
        String script = "MATCH(a:person)\n"
            + " Let Global a.weight = a.age / 10.0\n"
            + " Return a.id, a.weight\n";
        PlanTester
            .build()
            .registerGraph(GRAPH_G1)
            .gql(script)
            .toRel()
            .checkRelNode(
                "LogicalProject(id=[$0.id], weight=[$0.weight])\n"
                    + "  LogicalGraphMatch(path=[(a:person) PathModify([a=VERTEX{id:$0.id, ~label:$0.~label, name:$0"
                    + ".name, gender:$0.gender, age:$0.age, weight:CAST(/($0.age, 10.0)):"
                    + "JavaType(class java.math.BigDecimal)}])])\n"
                    + "    LogicalGraphScan(table=[default.g1])\n");
    }

    @Test
    public void testGQLLet3() {
        String script = "MATCH(a:person)"
            + " Let a.weight =  a.age / 100,"
            + "Let a.ext = 1"
            + "Return a.id, a.weight, a.ext";

        PlanTester
            .build()
            .registerGraph(GRAPH_G1)
            .gql(script)
            .toRel()
            .checkRelNode(
                "LogicalProject(id=[$0.id], weight=[$0.weight], ext=[$0.ext])\n"
                    + "  LogicalGraphMatch(path=[(a:person) PathModify([a=VERTEX{id:$0.id, ~label:$0.~label, name:$0"
                    + ".name, gender:$0.gender, age:$0.age, weight:CAST(/($0.age, 100)):JavaType(class java.lang"
                    + ".Integer)}]) PathModify([a=VERTEX{id:$0.id, ~label:$0.~label, name:$0.name, gender:$0.gender, "
                    + "age:$0.age, weight:$0.weight, ext:1}])])\n"
                    + "    LogicalGraphScan(table=[default.g1])\n");
    }

    @Test
    public void testContinueMatch() {
        String script = "Match(a)-[e1]->(b)\n"
            + "Let a.weight = e1.weight / 10\n"
            + "Match(b) - (c)";

        PlanTester
            .build()
            .gql(script)
            .toRel()
            .checkRelNode(
                "LogicalGraphMatch(path=[(a:)-[e1:]->(b:) PathModify([a=VERTEX{id:$0.id, ~label:$0.~label, "
                    + "name:$0.name, age:$0.age, weight:CAST(/($1.weight, 10)):JavaType(class java.lang.Double)}])"
                    + "-[e_col_2:]-(c:)])\n"
                    + "  LogicalGraphScan(table=[default.g0])\n");
    }

    @Test
    public void testGQLWith() {
        String script = "With p as (Select id from t0)\n"
            + "Match (a where a.id = p.id) -[e] -> (b)\n"
            + "Return a.id as a_id, b.id as b_id";

        PlanTester
            .build()
            .gql(script)
            .toRel()
            .checkRelNode(
                "LogicalParameterizedRelNode\n" + "  LogicalProject(id=[$0])\n"
                    + "    LogicalTableScan(table=[[default, t0]])\n"
                    + "  LogicalProject(a_id=[$0.id], b_id=[$2.id])\n"
                    + "    LogicalGraphMatch(path=[(a:) where =(a.id, CAST($$0):BIGINT) -[e:]->(b:)])\n"
                    + "      LogicalGraphScan(table=[default.g0])\n");
    }

    @Test
    public void testGQLFromWith() {
        String script = "Select a_id From(\n"
            + "With p as (Select id from t0)\n"
            + "Match (a where a.id = p.id) -[e] -> (b)\n"
            + "Return a.id as a_id, b.id as b_id"
            + ")";

        PlanTester
            .build()
            .gql(script)
            .toRel()
            .checkRelNode(
                "LogicalProject(a_id=[$0])\n" + "  LogicalParameterizedRelNode\n"
                    + "    LogicalProject(id=[$0])\n"
                    + "      LogicalTableScan(table=[[default, t0]])\n"
                    + "    LogicalProject(a_id=[$0.id], b_id=[$2.id])\n"
                    + "      LogicalGraphMatch(path=[(a:) where =(a.id, CAST($$0):BIGINT) -[e:]->(b:)])\n"
                    + "        LogicalGraphScan(table=[default.g0])\n");
    }

    @Test
    public void testGQLComplexMatchWithPathConcat() {
        String script1 = "Match(a)-(b), (b) - (c)"
            + "RETURN b, c, a.id";

        PlanTester
            .build()
            .gql(script1)
            .toRel()
            .checkRelNode(
                "LogicalProject(b=[$2], c=[$4], id=[$0.id])\n"
                    + "  LogicalGraphMatch(path=[(a:)-[e_col_1:]-(b:)-[e_col_3:]-(c:)])\n"
                    + "    LogicalGraphScan(table=[default.g0])\n");

        String script2 = "Match(b: person) - (c), (a)-(b: user)"
            + "RETURN b, c, a.id";

        PlanTester
            .build()
            .registerGraph(GRAPH_G1)
            .gql(script2)
            .toRel()
            .checkRelNode(
                "LogicalProject(b=[$2], c=[$4], id=[$0.id])\n"
                    + "  LogicalGraphMatch(path=[(a:)-[e_col_3:]-(b:user) where =(b.~label, _UTF-16LE'person') -[e_col_1:]-(c:)"
                    + "])\n"
                    + "    LogicalGraphScan(table=[default.g1])\n");

        String script3 = "Match(b: person) - (c where c.name = 'marko'), (a)-(b: user)"
            + "RETURN b, c, a.id";
        PlanTester
            .build()
            .registerGraph(GRAPH_G1)
            .gql(script3)
            .toRel()
            .checkRelNode(
                "LogicalProject(b=[$2], c=[$4], id=[$0.id])\n"
                    + "  LogicalGraphMatch(path=[(a:)-[e_col_3:]-(b:user) where =(b.~label, _UTF-16LE'person') -[e_col_1:]-"
                    + "(c:) where =(c.name, _UTF-16LE'marko') ])\n"
                    + "    LogicalGraphScan(table=[default.g1])\n");
    }

    @Test
    public void testGQLComplexMatchWithPathNotConcat() {
        String script1 = "Match(a)-(b), (b) - (c), (b) - (d) - (f)"
            + "RETURN b, c, a, d, f";

        PlanTester
            .build()
            .gql(script1)
            .toRel()
            .checkRelNode(
                "LogicalProject(b=[$2], c=[$4], a=[$0], d=[$7], f=[$9])\n"
                    + "  LogicalGraphMatch(path=[{(a:)-[e_col_1:]-(b:)-[e_col_3:]-(c:)} Join {(b:)-[e_col_5:]-(d:)"
                    + "-[e_col_6:]-(f:)}])\n"
                    + "    LogicalGraphScan(table=[default.g0])\n");

        String script2 = "Match(a:user where a.id = 0)-[e]-(b),(a where a.id = 2)-(c), (d) - (a)\n"
            + "RETURN a, b, c, d";

        PlanTester
            .build()
            .gql(script2)
            .toRel()
            .checkRelNode(
                "LogicalProject(a=[$2], b=[$4], c=[$7], d=[$0])\n"
                    + "  LogicalGraphMatch(path=[{(d:)-[e_col_4:]-(a:) where =(a.~label, _UTF-16LE'user')  where =(a.id, 0) "
                    + "-[e:]-(b:)} Join {(a:) where =(a.id, 2) -[e_col_2:]-(c:)}])\n"
                    + "    LogicalGraphScan(table=[default.g0])\n");

        String script3 = "Match(a)-(b where b.name = 'marko'), (c) - (d where d.id = 1)\n"
            + "Return d, c, a, b";

        PlanTester
            .build()
            .gql(script3)
            .toRel()
            .checkRelNode("LogicalProject(d=[$5], c=[$3], a=[$0], b=[$2])\n"
                + "  LogicalGraphMatch(path=[{(a:)-[e_col_1:]-(b:) where =(b.name, _UTF-16LE'marko') } Join {(c:)"
                + "-[e_col_3:]-(d:) where =(d.id, 1) }])\n"
                + "    LogicalGraphScan(table=[default.g0])\n");
    }

    @Test
    public void testGQLComplexMatchWithLet() {
        String script = "Match(a)-(b), (b) - (c)"
            + "Let a.weight = a.age / 10,"
            + "Let b.weight = b.age / 10"
            + "RETURN a, b, c.id";

        PlanTester
            .build()
            .gql(script)
            .toRel()
            .checkRelNode(
                "LogicalProject(a=[$0], b=[$2], id=[$4.id])\n"
                    + "  LogicalGraphMatch(path=[(a:)-[e_col_1:]-(b:)-[e_col_3:]-(c:) PathModify([a=VERTEX{id:$0.id, "
                    + "~label:$0.~label, name:$0.name, age:$0.age, weight:CAST(/($0.age, 10)):JavaType(class java"
                    + ".lang.Integer)}]) PathModify([b=VERTEX{id:$2.id, ~label:$2.~label, name:$2.name, age:$2.age, "
                    + "weight:CAST(/($2.age, 10)):JavaType(class java.lang.Integer)}])])\n"
                    + "    LogicalGraphScan(table=[default.g0])\n");
    }

    @Test
    public void testGQLLoopMatch() {
        String script1 = "MATCH (a:user where id = 1)-[e:knows where e.weight > 0.4]->{1, 5} (b:user)\n"
            + " RETURN a";
        PlanTester
            .build()
            .gql(script1)
            .toRel()
            .checkRelNode(
                "LogicalProject(a=[$0])\n"
                    + "  LogicalGraphMatch(path=[(a:user) where =(a.id, 1) - loop(-[e:knows]-> where >(e.weight, 0.4)"
                    + " (b:user)).time(1,5).until(true)])\n"
                    + "    LogicalGraphScan(table=[default.g0])\n");


        String script2 = "MATCH (a:user where id = 1)-[e:knows where e.weight > 0.4]->{1,} (b:user) -> (c)\n"
            + " RETURN a, c";
        PlanTester
            .build()
            .gql(script2)
            .toRel()
            .checkRelNode(
                "LogicalProject(a=[$0], c=[$4])\n"
                    + "  LogicalGraphMatch(path=[(a:user) where =(a.id, 1) - loop(-[e:knows]-> where >(e.weight, 0.4)"
                    + " (b:user)).time(1,-1).until(true)-[e_col_1:]->(c:)])\n"
                    + "    LogicalGraphScan(table=[default.g0])\n");
    }

    @Test
    public void testGQLSubQuery() {
        String script1 = "MATCH (a:user where id = 1)-[e:knows where e.weight > 0.4]-> (b:user)\n"
            + " Where AVG((b) ->(c) => c.age) > 20\n"
            + " RETURN b";
        PlanTester
            .build()
            .gql(script1)
            .toRel()
            .checkRelNode(
                "LogicalProject(b=[$2])\n"
                    + "  LogicalGraphMatch(path=[(a:user) where =(a.id, 1) -[e:knows]-> where >(e.weight, 0.4) "
                    + "(b:user) where >(AVG((b:)-[e_col_2:]->(c:) => $4.age), 20) ])\n"
                    + "    LogicalGraphScan(table=[default.g0])\n");


        String script2 = "MATCH (a:user where id = 1)-[e:knows where e.weight > 0.4]-> (b:user)\n"
            + " Where SUM((b) ->(c) => c.age) > 20 AND COUNT((b) -(c) => c) > 10\n"
            + " RETURN b";
        PlanTester
            .build()
            .gql(script2)
            .toRel()
            .checkRelNode("LogicalProject(b=[$2])\n"
                + "  LogicalGraphMatch(path=[(a:user) where =(a.id, 1) -[e:knows]-> where >(e.weight, 0.4) (b:user) "
                + "where AND(>(SUM((b:)-[e_col_2:]->(c:) => $4.age), 20), >(COUNT((b:)-[e_col_4:]-(c:) => $4), 10)) "
                + "])\n"
                + "    LogicalGraphScan(table=[default.g0])\n");
    }

    @Test
    public void testGraphAlgorithm() {
        PlanTester.build()
            .gql("CALL SSSP(1) YIELD (vid, distance)\n" + "RETURN vid, distance")
            .toRel()
            .checkRelNode(
                "LogicalProject(vid=[$0], distance=[$1])\n"
                    + "  LogicalGraphAlgorithm(algo=[SingleSourceShortestPath], params=[[Integer]], "
                    + "outputType=[RecordType:peek(BIGINT id, BIGINT distance)])\n"
                    + "    LogicalGraphScan(table=[default.g0])\n"
            );
        PlanTester.build()
            .gql("CALL SSSP() YIELD (vid, distance)\n" + "RETURN vid, distance")
            .toRel()
            .checkRelNode(
                "LogicalProject(vid=[$0], distance=[$1])\n"
                    + "  LogicalGraphAlgorithm(algo=[SingleSourceShortestPath], params=[[]], "
                    + "outputType=[RecordType:peek(BIGINT id, BIGINT distance)])\n"
                    + "    LogicalGraphScan(table=[default.g0])\n"
            );
    }
}
