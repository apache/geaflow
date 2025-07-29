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

package org.apache.geaflow.dsl.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.util.SqlVisitor;

public class GQLNodeUtil {

    public static <T extends SqlNode> List<T> collect(SqlNode node, Predicate<SqlNode> predicate) {
        return node.accept(new SqlVisitor<List<T>>() {
            @Override
            public List<T> visit(SqlLiteral literal) {
                if (predicate.test(literal)) {
                    return (List<T>) Collections.singletonList(literal);
                }
                return Collections.emptyList();
            }

            @Override
            public List<T> visit(SqlCall call) {
                List<T> childResults = call.getOperandList()
                    .stream()
                    .filter(Objects::nonNull)
                    .flatMap(operand -> operand.accept(this).stream())
                    .collect(Collectors.toList());

                List<T> results = new ArrayList<>();
                results.addAll(childResults);

                if (predicate.test(call)) {
                    results.add((T) call);
                }
                return results;
            }

            @Override
            public List<T> visit(SqlNodeList nodeList) {
                List<T> childResults = nodeList.getList()
                    .stream()
                    .filter(Objects::nonNull)
                    .flatMap(operand -> operand.accept(this).stream())
                    .collect(Collectors.toList());

                List<T> results = new ArrayList<>();
                results.addAll(childResults);

                if (predicate.test(nodeList)) {
                    results.add((T) nodeList);
                }
                return results;
            }

            @Override
            public List<T> visit(SqlIdentifier id) {
                if (predicate.test(id)) {
                    return (List<T>) Collections.singletonList(id);
                }
                return Collections.emptyList();
            }

            @Override
            public List<T> visit(SqlDataTypeSpec type) {
                if (predicate.test(type)) {
                    return (List<T>) Collections.singletonList(type);
                }
                return Collections.emptyList();
            }

            @Override
            public List<T> visit(SqlDynamicParam param) {
                if (predicate.test(param)) {
                    return (List<T>) Collections.singletonList(param);
                }
                return Collections.emptyList();
            }

            @Override
            public List<T> visit(SqlIntervalQualifier intervalQualifier) {
                if (predicate.test(intervalQualifier)) {
                    return (List<T>) Collections.singletonList(intervalQualifier);
                }
                return Collections.emptyList();
            }
        });
    }

    public static boolean containMatch(SqlNode node) {
        return !collect(node, n -> n.getKind() == SqlKind.GQL_MATCH_PATTERN).isEmpty();
    }

    public static SqlNode and(SqlNode... filters) {
        List<SqlNode> nonNullFilters =
            Arrays.stream(filters)
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
        if (nonNullFilters.size() == 0) {
            return SqlLiteral.createBoolean(true, SqlParserPos.ZERO);
        }
        if (nonNullFilters.size() == 1) {
            return nonNullFilters.get(0);
        }
        SqlNode and = null;
        for (SqlNode filter : nonNullFilters) {
            if (and == null) {
                and = filter;
            } else {
                and = SqlStdOperatorTable.AND.createCall(SqlParserPos.ZERO, and, filter);
            }
        }
        return and;
    }

    public static SqlIdentifier getGraphTableName(SqlIdentifier completeIdentifier) {
        assert completeIdentifier.names.size() >= 2;
        return completeIdentifier.getComponent(0, 2);
    }
}
