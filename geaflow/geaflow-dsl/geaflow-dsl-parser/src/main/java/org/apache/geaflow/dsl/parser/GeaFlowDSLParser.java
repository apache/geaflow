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

package org.apache.geaflow.dsl.parser;

import java.io.StringReader;
import java.util.List;
import org.apache.calcite.config.Lex;
import org.apache.calcite.runtime.CalciteContextException;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.impl.GeaFlowParserImpl;

public class GeaFlowDSLParser {

    public static SqlParser.Config PARSER_CONFIG =
        SqlParser.configBuilder()
            .setLex(Lex.MYSQL)
            .setParserFactory(GeaFlowParserImpl.FACTORY)
            .setConformance(GQLConformance.INSTANCE)
            .build();

    public List<SqlNode> parseMultiStatement(String sql) throws SqlParseException {
        GeaFlowParserImpl parser = createParser(sql);
        try {
            return parser.MultiStmtEof();
        } catch (Throwable ex) {
            if (ex instanceof CalciteContextException) {
                ((CalciteContextException) ex).setOriginalStatement(sql);
            }
            throw parser.normalizeException(ex);
        }
    }

    public SqlNode parseStatement(String sql) throws SqlParseException {
        GeaFlowParserImpl parser = createParser(sql);
        try {
            return parser.parseSqlStmtEof();
        } catch (Throwable ex) {
            if (ex instanceof CalciteContextException) {
                ((CalciteContextException) ex).setOriginalStatement(sql);
            }
            throw parser.normalizeException(ex);
        }
    }

    private GeaFlowParserImpl createParser(String sql) {
        GeaFlowParserImpl parser = (GeaFlowParserImpl) PARSER_CONFIG.parserFactory()
            .getParser(new StringReader(sql));

        parser.setOriginalSql(sql);
        parser.setTabSize(1);
        parser.setQuotedCasing(PARSER_CONFIG.quotedCasing());
        parser.setUnquotedCasing(PARSER_CONFIG.unquotedCasing());
        parser.setIdentifierMaxLength(PARSER_CONFIG.identifierMaxLength());
        parser.setConformance(PARSER_CONFIG.conformance());
        switch (PARSER_CONFIG.quoting()) {
            case DOUBLE_QUOTE:
                parser.switchTo("DQID");
                break;
            case BACK_TICK:
                parser.switchTo("BTID");
                break;
            default:
                parser.switchTo("DEFAULT");
        }

        return parser;
    }
}
