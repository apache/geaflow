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

package org.apache.geaflow.dsl.sqlnode;

import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.geaflow.dsl.operator.SqlMatchEdgeOperator;

public class SqlMatchEdge extends SqlMatchNode {

    private final EdgeDirection direction;

    private final int minHop;

    private final int maxHop;

    private SqlNode sourceCondition;

    private SqlNode destCondition;

    public SqlMatchEdge(SqlParserPos pos, SqlIdentifier name,
                        SqlNodeList labels, SqlNodeList propertySpecification, SqlNode where,
                        SqlNode sourceCondition, SqlNode destCondition,
                        EdgeDirection direction,
                        int minHop, int maxHop) {
        super(pos, name, labels, propertySpecification, where);
        this.sourceCondition = sourceCondition;
        this.destCondition = destCondition;
        this.direction = direction;
        this.minHop = minHop;
        this.maxHop = maxHop;
    }

    // Constructor for backward compatibility
    public SqlMatchEdge(SqlParserPos pos, SqlIdentifier name,
                        SqlNodeList labels, SqlNodeList propertySpecification, SqlNode where,
                        EdgeDirection direction,
                        int minHop, int maxHop) {
        this(pos, name, labels, propertySpecification, where, null, null, direction, minHop, maxHop);
    }

    @Override
    public SqlOperator getOperator() {
        return SqlMatchEdgeOperator.INSTANCE;
    }

    @Override
    public SqlKind getKind() {
        return SqlKind.GQL_MATCH_EDGE;
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        if (getName() == null && getLabels() == null && getWhere() == null
            && sourceCondition == null && destCondition == null) {
            switch (direction) {
                case IN:
                    writer.print("<-");
                    break;
                case OUT:
                    writer.print("->");
                    break;
                case BOTH:
                    writer.print("-");
                    break;
                default:
                    throw new IllegalArgumentException("Illegal direction: " + direction);
            }
        } else {
            if (direction == EdgeDirection.IN) {
                writer.print("<");
            }
            writer.print("-[");
            unparseNode(writer);
            
            // Add source/destination conditions
            if (sourceCondition != null || destCondition != null) {
                if (sourceCondition != null) {
                    writer.keyword(", SOURCE");
                    sourceCondition.unparse(writer, 0, 0);
                }
                if (destCondition != null) {
                    writer.keyword(", DESTINATION");
                    destCondition.unparse(writer, 0, 0);
                }
            }
            
            writer.print("]-");
            if (direction == EdgeDirection.OUT) {
                writer.print(">");
            }
            if (minHop != -1 || maxHop != -1) {
                writer.print("{");
                if (minHop != -1) {
                    writer.print(minHop);
                }
                writer.print(",");
                if (maxHop != -1) {
                    writer.print(maxHop);
                }
                writer.print("}");
            }
        }

    }

    public EdgeDirection getDirection() {
        return direction;
    }

    public enum EdgeDirection {
        OUT,
        IN,
        BOTH;

        public static EdgeDirection of(String value) {
            for (EdgeDirection direction : EdgeDirection.values()) {
                if (direction.name().equalsIgnoreCase(value)) {
                    return direction;
                }
            }
            throw new IllegalArgumentException("Illegal direction value: " + value);
        }

        public static EdgeDirection reverse(EdgeDirection direction) {
            return direction == BOTH ? direction : ((direction == IN) ? OUT : IN);
        }
    }

    @Override
    public void validate(SqlValidator validator, SqlValidatorScope scope) {
        validator.validateQuery(this, scope, validator.getUnknownType());
    }

    public int getMinHop() {
        return minHop;
    }

    public int getMaxHop() {
        return maxHop;
    }

    public boolean isRegexMatch() {
        return minHop != 1 || maxHop != 1;
    }

    public SqlNode getSourceCondition() {
        return sourceCondition;
    }

    public SqlNode getDestCondition() {
        return destCondition;
    }

    public void setSourceCondition(SqlNode sourceCondition) {
        this.sourceCondition = sourceCondition;
    }

    public void setDestCondition(SqlNode destCondition) {
        this.destCondition = destCondition;
    }
}
