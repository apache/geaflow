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

package org.apache.geaflow.dsl.schema.function;

import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.util.ReflectiveSqlOperatorTable;

public class BuildInSqlOperatorTable extends ReflectiveSqlOperatorTable {

    public static final SqlAggFunction MIN = new GqlMinMaxAggFunction(SqlKind.MIN);
    public static final SqlAggFunction MAX = new GqlMinMaxAggFunction(SqlKind.MAX);
    public static final SqlAggFunction SUM = new GqlSumAggFunction(null);
    public static final SqlAggFunction COUNT = new GqlCountAggFunction("COUNT");
    public static final SqlAggFunction AVG = new GqlAvgAggFunction(SqlKind.AVG);

    private final SqlOperator[] buildInSqlOperators = {
        // SET OPERATORS
        SqlStdOperatorTable.UNION,
        SqlStdOperatorTable.UNION_ALL,
        SqlStdOperatorTable.EXCEPT,
        SqlStdOperatorTable.EXCEPT_ALL,
        SqlStdOperatorTable.INTERSECT,
        SqlStdOperatorTable.INTERSECT_ALL,
        // BINARY OPERATORS
        SqlStdOperatorTable.AND,
        SqlStdOperatorTable.AS,
        SqlStdOperatorTable.CONCAT,
        GeaFlowOverwriteSqlOperators.DIVIDE,
        SqlStdOperatorTable.DOT,
        SqlStdOperatorTable.EQUALS,
        SqlStdOperatorTable.GREATER_THAN,
        SqlStdOperatorTable.IS_DISTINCT_FROM,
        SqlStdOperatorTable.IS_NOT_DISTINCT_FROM,
        SqlStdOperatorTable.GREATER_THAN_OR_EQUAL,
        SqlStdOperatorTable.LESS_THAN,
        SqlStdOperatorTable.LESS_THAN_OR_EQUAL,
        GeaFlowOverwriteSqlOperators.MINUS,
        GeaFlowOverwriteSqlOperators.MULTIPLY,
        SqlStdOperatorTable.NOT_EQUALS,
        SqlStdOperatorTable.OR,
        GeaFlowOverwriteSqlOperators.PLUS,
        SqlStdOperatorTable.DATETIME_PLUS,
        // POSTFIX OPERATORS
        SqlStdOperatorTable.DESC,
        SqlStdOperatorTable.NULLS_FIRST,
        SqlStdOperatorTable.IS_NOT_NULL,
        SqlStdOperatorTable.IS_NULL,
        SqlStdOperatorTable.IS_NOT_TRUE,
        SqlStdOperatorTable.IS_TRUE,
        SqlStdOperatorTable.IS_NOT_FALSE,
        SqlStdOperatorTable.IS_FALSE,
        SqlStdOperatorTable.IS_NOT_UNKNOWN,
        SqlStdOperatorTable.IS_UNKNOWN,
        // PREFIX OPERATORS
        SqlStdOperatorTable.NOT,
        SqlStdOperatorTable.UNARY_MINUS,
        SqlStdOperatorTable.UNARY_PLUS,
        // GROUPING FUNCTIONS
        SqlStdOperatorTable.GROUP_ID,
        SqlStdOperatorTable.GROUPING,
        SqlStdOperatorTable.GROUPING_ID,
        // AGGREGATE OPERATORS
        BuildInSqlOperatorTable.SUM,
        SqlStdOperatorTable.SUM0,
        BuildInSqlOperatorTable.COUNT,
        BuildInSqlOperatorTable.MIN,
        BuildInSqlOperatorTable.MAX,
        BuildInSqlOperatorTable.AVG,
        SqlStdOperatorTable.STDDEV_POP,
        SqlStdOperatorTable.STDDEV_SAMP,
        SqlStdOperatorTable.VAR_POP,
        SqlStdOperatorTable.VAR_SAMP,
        // ARRAY OPERATORS
        SqlStdOperatorTable.ARRAY_VALUE_CONSTRUCTOR,
        SqlStdOperatorTable.ITEM,
        SqlStdOperatorTable.CARDINALITY,
        SqlStdOperatorTable.ELEMENT,
        // SPECIAL OPERATORS
        SqlStdOperatorTable.ROW,
        SqlStdOperatorTable.OVERLAPS,
        SqlStdOperatorTable.LITERAL_CHAIN,
        SqlStdOperatorTable.BETWEEN,
        SqlStdOperatorTable.SYMMETRIC_BETWEEN,
        SqlStdOperatorTable.NOT_BETWEEN,
        SqlStdOperatorTable.SYMMETRIC_NOT_BETWEEN,
        SqlStdOperatorTable.NOT_LIKE,
        SqlStdOperatorTable.LIKE,
        SqlStdOperatorTable.NOT_SIMILAR_TO,
        SqlStdOperatorTable.SIMILAR_TO,
        SqlStdOperatorTable.CASE,
        SqlStdOperatorTable.REINTERPRET,
        // FUNCTIONS
        SqlStdOperatorTable.SUBSTRING,
        SqlStdOperatorTable.OVERLAY,
        SqlStdOperatorTable.TRIM,
        SqlStdOperatorTable.POSITION,
        SqlStdOperatorTable.CHAR_LENGTH,
        SqlStdOperatorTable.CHARACTER_LENGTH,
        SqlStdOperatorTable.UPPER,
        SqlStdOperatorTable.LOWER,
        SqlStdOperatorTable.INITCAP,
        SqlStdOperatorTable.POWER,
        SqlStdOperatorTable.SQRT,
        GeaFlowOverwriteSqlOperators.MOD,
        GeaFlowOverwriteSqlOperators.PERCENT_REMAINDER,
        SqlStdOperatorTable.LN,
        SqlStdOperatorTable.LOG10,
        SqlStdOperatorTable.ABS,
        SqlStdOperatorTable.EXP,
        SqlStdOperatorTable.NULLIF,
        SqlStdOperatorTable.COALESCE,
        SqlStdOperatorTable.FLOOR,
        SqlStdOperatorTable.CEIL,
        SqlStdOperatorTable.LOCALTIME,
        SqlStdOperatorTable.LOCALTIMESTAMP,
        SqlStdOperatorTable.CURRENT_TIME,
        SqlStdOperatorTable.CURRENT_TIMESTAMP,
        SqlStdOperatorTable.CURRENT_DATE,
        SqlStdOperatorTable.TIMESTAMP_ADD,
        SqlStdOperatorTable.TIMESTAMP_DIFF,
        SqlStdOperatorTable.CAST,
        SqlStdOperatorTable.EXTRACT,
        SqlStdOperatorTable.SCALAR_QUERY,
        SqlStdOperatorTable.EXISTS,
        SqlStdOperatorTable.SIN,
        SqlStdOperatorTable.COS,
        SqlStdOperatorTable.TAN,
        SqlStdOperatorTable.COT,
        SqlStdOperatorTable.ASIN,
        SqlStdOperatorTable.ACOS,
        SqlStdOperatorTable.ATAN,
        SqlStdOperatorTable.DEGREES,
        SqlStdOperatorTable.RADIANS,
        GeaFlowOverwriteSqlOperators.SIGN,
        // SqlStdOperatorTable.ROUND,
        SqlStdOperatorTable.PI,
        SqlStdOperatorTable.RAND,
        SqlStdOperatorTable.RAND_INTEGER,
        // EXTENSIONS
        SqlStdOperatorTable.TUMBLE,
        SqlStdOperatorTable.TUMBLE_START,
        SqlStdOperatorTable.TUMBLE_END,
        SqlStdOperatorTable.HOP,
        SqlStdOperatorTable.HOP_START,
        SqlStdOperatorTable.HOP_END,
        SqlStdOperatorTable.SESSION,
        SqlStdOperatorTable.SESSION_START,
        SqlStdOperatorTable.SESSION_END,

        SqlStdOperatorTable.RANK,
        SqlStdOperatorTable.PERCENT_RANK,
        SqlStdOperatorTable.DENSE_RANK,
        SqlStdOperatorTable.CUME_DIST,
        SqlStdOperatorTable.ROW_NUMBER,
        SqlStdOperatorTable.LAG,
        SqlStdOperatorTable.LEAD
    };

    public BuildInSqlOperatorTable() {
        this.register();
    }

    private void register() {
        for (SqlOperator operator : buildInSqlOperators) {
            super.register(operator);
        }
    }

}
