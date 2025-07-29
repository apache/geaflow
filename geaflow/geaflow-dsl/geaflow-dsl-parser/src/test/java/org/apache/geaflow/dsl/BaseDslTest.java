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

import com.google.common.io.Resources;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlDialect.DatabaseProduct;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.pretty.SqlPrettyWriter;
import org.apache.calcite.sql.util.SqlString;
import org.apache.calcite.tools.ValidationException;
import org.apache.geaflow.dsl.parser.GeaFlowDSLParser;
import org.apache.geaflow.dsl.sqlnode.SqlCreateTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BaseDslTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(BaseDslTest.class);

    public List<SqlNode> parseSql(String path) throws Exception {
        URL url = Resources.getResource(path);
        String sql = Resources.toString(url, Charset.defaultCharset());
        return parseStmts(sql);
    }

    public String parseSqlAndUnParse(String path) throws Exception {
        URL url = Resources.getResource(path);
        String sql = Resources.toString(url, Charset.defaultCharset());
        return parseStmtsAndUnParse(sql);
    }

    public List<SqlNode> parseStmts(String stmts) throws Exception {
        LOGGER.info("Origin Sql:\n" + stmts);
        GeaFlowDSLParser parser = new GeaFlowDSLParser();
        List<SqlNode> sqlNodes = parser.parseMultiStatement(stmts);
        return sqlNodes;
    }

    public String parseStmtsAndUnParse(String stmts) throws Exception {
        LOGGER.info("Origin Sql:\n" + stmts);
        GeaFlowDSLParser parser = new GeaFlowDSLParser();
        List<SqlNode> sqlNodes = parser.parseMultiStatement(stmts);
        checkSqlNodes(sqlNodes);
        String unParseSql = unparse(sqlNodes);
        LOGGER.info("Unparse Sql:\n" + unParseSql);
        return unParseSql;
    }

    private void checkSqlNodes(List<SqlNode> sqlNodes) throws ValidationException {
        Set<SqlNode> nodeSet = new LinkedHashSet();
        for (SqlNode node : sqlNodes) {
            nodeSet.add(node);
        }
        for (SqlNode node : nodeSet) {
            if (node instanceof SqlCall) {
                SqlCall call = (SqlCall) node;
                List<SqlNode> operandList = call.getOperandList();
                checkSqlNodes(operandList);
                for (int i = 0; i < operandList.size(); i++) {
                    call.setOperand(i, operandList.get(i));
                }
            } else if (node instanceof SqlNodeList) {
                checkSqlNodes(((SqlNodeList) node).getList());
            }
            if (node instanceof SqlCreateTable) {
                ((SqlCreateTable) node).validate();
            }
        }
    }

    /**
     * Unparse multiple SqlNode to standard sql statement.
     */
    public String unparse(List<SqlNode> sqlNodes) {
        StringBuilder builder = new StringBuilder();
        for (SqlNode node : sqlNodes) {
            builder
                .append(toSqlString(node, DatabaseProduct.UNKNOWN.getDialect(),
                    false).toString());
            builder.append(";");
            builder.append("\n");
            builder.append("\n");
        }
        return builder.toString();
    }

    public SqlString toSqlString(SqlNode sqlNode, SqlDialect dialect,
                                 boolean forceParens) {
        if (dialect == null) {
            dialect = SqlDialect.DUMMY;
        }
        SqlPrettyWriter writer = new SqlPrettyWriter(dialect);
        writer.setAlwaysUseParentheses(forceParens);
        writer.setQuoteAllIdentifiers(false);
        writer.setIndentation(0);
        writer.setSelectListItemsOnSeparateLines(true);
        sqlNode.unparse(writer, 0, 0);
        final String sql = writer.toString();
        return new SqlString(dialect, sql);
    }

}
