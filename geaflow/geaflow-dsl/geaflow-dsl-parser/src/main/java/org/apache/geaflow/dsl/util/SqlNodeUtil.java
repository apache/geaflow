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
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.util.SqlVisitor;

public class SqlNodeUtil {

    public static void unparseNodeList(SqlWriter writer,
                                       SqlNodeList nodeList, String sep) {
        for (int i = 0; i < nodeList.size(); i++) {
            if (i > 0) {
                writer.print(sep);
                writer.newlineAndIndent();
            }
            nodeList.get(i).unparse(writer, 0, 0);
        }
    }

    public static List<SqlUnresolvedFunction> findUnresolvedFunctions(SqlNode sqlNode) {
        return sqlNode.accept(new SqlVisitor<List<SqlUnresolvedFunction>>() {
            @Override
            public List<SqlUnresolvedFunction> visit(SqlLiteral literal) {
                return Collections.emptyList();
            }

            @Override
            public List<SqlUnresolvedFunction> visit(SqlCall call) {
                List<SqlUnresolvedFunction> functions = new ArrayList<>();
                if (call.getOperator() instanceof SqlUnresolvedFunction) {
                    functions.add((SqlUnresolvedFunction) call.getOperator());
                }
                functions.addAll(visitNodes(call.getOperandList()));
                return functions;
            }

            @Override
            public List<SqlUnresolvedFunction> visit(SqlNodeList nodeList) {
                return visitNodes(nodeList.getList());
            }

            private List<SqlUnresolvedFunction> visitNodes(List<SqlNode> nodes) {
                if (nodes == null) {
                    return Collections.emptyList();
                }
                return nodes.stream()
                    .flatMap(node -> {
                        if (node != null) {
                            return node.accept(this).stream();
                        } else {
                            return Collections.<SqlUnresolvedFunction>emptyList().stream();
                        }
                    })
                    .collect(Collectors.toList());
            }

            @Override
            public List<SqlUnresolvedFunction> visit(SqlIdentifier id) {
                return Collections.emptyList();
            }

            @Override
            public List<SqlUnresolvedFunction> visit(SqlDataTypeSpec type) {
                return Collections.emptyList();
            }

            @Override
            public List<SqlUnresolvedFunction> visit(SqlDynamicParam param) {
                return Collections.emptyList();
            }

            @Override
            public List<SqlUnresolvedFunction> visit(SqlIntervalQualifier intervalQualifier) {
                return Collections.emptyList();
            }
        });
    }

    public static Set<String> findUsedTables(SqlNode sqlNode) {
        return sqlNode.accept(new SqlVisitor<Set<String>>() {
            @Override
            public Set<String> visit(SqlLiteral literal) {
                return Collections.emptySet();
            }

            @Override
            public Set<String> visit(SqlCall call) {
                Set<String> allTables = new HashSet<>();
                if (call instanceof SqlInsert) {
                    SqlInsert sqlInsert = (SqlInsert) call;
                    SqlNode source = sqlInsert.getSource();
                    Set<String> sourceTables = source.accept(this);
                    SqlNode target = sqlInsert.getTargetTable();
                    Set<String> targetTables = target.accept(this);
                    allTables.addAll(sourceTables);
                    allTables.addAll(targetTables);
                } else if (call instanceof SqlSelect) {
                    SqlSelect sqlSelect = (SqlSelect) call;
                    SqlNode from = sqlSelect.getFrom();
                    if (from != null) {
                        Set<String> tables = from.accept(this);
                        allTables.addAll(tables);
                    }
                } else if (call instanceof SqlJoin) {
                    SqlJoin sqlJoin = (SqlJoin) call;
                    String left = sqlJoin.getLeft().toString();
                    String right = sqlJoin.getRight().toString();
                    allTables.add(left);
                    allTables.add(right);
                } else if (call instanceof SqlWith) {
                    SqlWith sqlWith = (SqlWith) call;
                    SqlNodeList withList = sqlWith.withList;
                    if (withList != null) {
                        Set<String> tables = withList.accept(this);
                        allTables.addAll(tables);
                    }

                } else if (call instanceof SqlWithItem) {
                    SqlWithItem withItem = (SqlWithItem) call;
                    allTables.add(withItem.name.names.get(0));
                    SqlNode query = withItem.query;
                    if (query != null) {
                        Set<String> tables = query.accept(this);
                        allTables.addAll(tables);
                    }

                } else if (call instanceof SqlBasicCall) {
                    SqlBasicCall basicCall = (SqlBasicCall) call;
                    SqlNode[] operands = basicCall.getOperands();
                    if (operands.length > 0) {
                        Set<String> tables = operands[0].accept(this);
                        allTables.addAll(tables);
                    }
                }
                return allTables;
            }

            @Override
            public Set<String> visit(SqlNodeList nodeList) {
                return visitNodes(nodeList.getList());
            }

            private Set<String> visitNodes(List<SqlNode> nodes) {
                if (nodes == null) {
                    return Collections.emptySet();
                }
                return nodes.stream()
                    .flatMap(node -> {
                        if (node != null) {
                            return node.accept(this).stream();
                        } else {
                            return Collections.<String>emptySet().stream();
                        }
                    })
                    .collect(Collectors.toSet());
            }

            @Override
            public Set<String> visit(SqlIdentifier id) {
                if (!id.names.isEmpty()) {
                    String name = id.names.get(0);
                    return Collections.singleton(name);
                } else {
                    return Collections.emptySet();
                }

            }

            @Override
            public Set<String> visit(SqlDataTypeSpec type) {
                return Collections.emptySet();
            }

            @Override
            public Set<String> visit(SqlDynamicParam param) {
                return Collections.emptySet();
            }

            @Override
            public Set<String> visit(SqlIntervalQualifier intervalQualifier) {
                return Collections.emptySet();
            }
        });
    }
}
