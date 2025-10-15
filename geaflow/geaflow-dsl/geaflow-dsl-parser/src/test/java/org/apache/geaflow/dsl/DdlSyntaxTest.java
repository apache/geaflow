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

import org.testng.Assert;
import org.testng.annotations.Test;

@Test(groups = "SyntaxTest")
public class DdlSyntaxTest extends BaseDslTest {

    @Test
    public void testGQLCreateFunction() throws Exception {
        String unParseSql = parseSqlAndUnParse("GQLCreateFunction.sql");
        String unParseStmts = parseStmtsAndUnParse(parseStmtsAndUnParse(unParseSql));
        Assert.assertEquals(unParseStmts, unParseSql);
    }

    @Test
    public void testGQLCreateGraph() throws Exception {
        String unParseSql = parseSqlAndUnParse("GQLCreateGraph.sql");
        String unParseStmts = parseStmtsAndUnParse(parseStmtsAndUnParse(unParseSql));
        Assert.assertEquals(unParseStmts, unParseSql);
    }

    @Test
    public void testGQLCreateGraphUsing() throws Exception {
        String unParseSql = parseSqlAndUnParse("GQLCreateGraphUsing.sql");
        String unParseStmts = parseStmtsAndUnParse(parseStmtsAndUnParse(unParseSql));
        Assert.assertEquals(unParseStmts, unParseSql);
    }

    @Test
    public void testGQLAlterGraph() throws Exception {
        String unParseSql = parseSqlAndUnParse("GQLAlterGraph.sql");
        String unParseStmts = parseStmtsAndUnParse(parseStmtsAndUnParse(unParseSql));
        Assert.assertEquals(unParseStmts, unParseSql);
    }

    @Test
    public void testCreateTable() throws Exception {
        String unParseSql = parseSqlAndUnParse("CreateTable.sql");
        String unParseStmts = parseStmtsAndUnParse(parseStmtsAndUnParse(unParseSql));
        Assert.assertEquals(unParseStmts, unParseSql);
    }

    @Test
    public void testGQLDescGraph() throws Exception {
        String unParseSql = parseSqlAndUnParse("GQLDescGraph.sql");
        String unParseStmts = parseStmtsAndUnParse(parseStmtsAndUnParse(unParseSql));
        Assert.assertEquals(unParseStmts, unParseSql);
    }

    @Test
    public void testCreateView() throws Exception {
        String unParseSql = parseSqlAndUnParse("CreateView.sql");
        String unParseStmts = parseStmtsAndUnParse(parseStmtsAndUnParse(unParseSql));
        Assert.assertEquals(unParseStmts, unParseSql);
    }

    @Test
    public void testGQLLet() throws Exception {
        String unParseSql = parseSqlAndUnParse("GQLLetStatement.sql");
        String unParseStmts = parseStmtsAndUnParse(parseStmtsAndUnParse(unParseSql));
        Assert.assertEquals(unParseStmts, unParseSql);
    }

    @Test
    public void testUseInstance() throws Exception {
        String unParseSql = parseSqlAndUnParse("UseInstance.sql");
        String unParseStmts = parseStmtsAndUnParse(parseStmtsAndUnParse(unParseSql));
        Assert.assertEquals(unParseStmts, unParseSql);
    }

    @Test
    public void testEdgeConstraint() throws Exception {
        String unParseSql = parseSqlAndUnParse("GQLEdgeConstraint.sql");
        String unParseStmts = parseStmtsAndUnParse(parseStmtsAndUnParse(unParseSql));
        Assert.assertEquals(unParseStmts, unParseSql);
    }
}
