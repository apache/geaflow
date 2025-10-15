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

package org.apache.geaflow.dsl.validator.namespace;

import static org.apache.calcite.util.Static.RESOURCE;

import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.sql.validate.SqlValidatorImpl;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.geaflow.dsl.calcite.PathRecordType;
import org.apache.geaflow.dsl.common.exception.GeaFlowDSLException;
import org.apache.geaflow.dsl.sqlnode.SqlMatchEdge;
import org.apache.geaflow.dsl.sqlnode.SqlMatchNode;
import org.apache.geaflow.dsl.validator.scope.GQLPathPatternScope;

public class GQLMatchNodeNamespace extends GQLBaseNamespace {

    private final SqlMatchNode matchNode;

    private MatchNodeContext matchNodeContext;

    public GQLMatchNodeNamespace(SqlValidatorImpl validator, SqlMatchNode matchNode) {
        super(validator, matchNode);
        this.matchNode = matchNode;
    }

    public void setMatchNodeContext(MatchNodeContext matchNodeContext) {
        this.matchNodeContext = matchNodeContext;
    }

    @Override
    protected RelDataType validateImpl(RelDataType targetRowType) {
        assert matchNodeContext != null : "matchNodeContext is null.";
        if (matchNode instanceof SqlMatchEdge) {
            SqlMatchEdge matchEdge = (SqlMatchEdge) matchNode;
            if (matchEdge.getMinHop() < 0) {
                throw new GeaFlowDSLException(matchEdge.getParserPosition(),
                    "The min hop:{} count should greater than 0.", matchEdge.getMinHop());
            }
            if (matchEdge.getMaxHop() != -1 && matchEdge.getMaxHop() < matchEdge.getMinHop()) {
                throw new GeaFlowDSLException(matchEdge.getParserPosition(),
                    "The max hop: {} count should greater than min hop: {}.", matchEdge.getMaxHop(),
                    matchEdge.getMinHop());
            }
        }
        GQLPathPatternScope pathPatternScope = matchNodeContext.getPathPatternScope();
        RelDataType nodeType = pathPatternScope.resolveTypeByLabels(matchNode, matchNodeContext);
        PathRecordType inputPathType = matchNodeContext.getInputPathType();
        PathRecordType outputPathType = inputPathType.addField(matchNode.getName(), nodeType, isCaseSensitive());
        // set outputPathType as the input for the next match node.
        matchNodeContext.setInputPathType(outputPathType);
        // register match node type
        getValidator().registerMatchNodeType(matchNode, nodeType);

        setType(outputPathType);

        if (matchNode.getWhere() != null) {
            SqlNode where = matchNode.getWhere();

            SqlValidatorScope whereScope = getValidator().getScopes(matchNode.getWhere());
            // expand where expression.
            SqlNode expandWhere = getValidator().expand(where, whereScope);
            if (expandWhere != where) {
                matchNode.setWhere(expandWhere);
                getValidator().registerScope(expandWhere, whereScope);
                where = matchNode.getWhere();
            }

            RelDataType boolType = getValidator().getTypeFactory().createSqlType(SqlTypeName.BOOLEAN);
            getValidator().inferUnknownTypes(boolType, whereScope, where);
            where.validate(getValidator(), whereScope);
            RelDataType conditionType = getValidator().deriveType(whereScope, where);
            if (!SqlTypeUtil.inBooleanFamily(conditionType)) {
                throw validator.newValidationError(where, RESOURCE.condMustBeBoolean("Filter"));
            }
        }
        return outputPathType;
    }

    @Override
    public SqlNode getNode() {
        return matchNode;
    }

    public RelDataType getNodeType() {
        return getValidator().getMatchNodeType(matchNode);
    }

    public static class MatchNodeContext {

        private GQLPathPatternScope pathPatternScope;

        private boolean isFirstNode;

        /**
         * The input path record type for current match node.
         */
        private PathRecordType inputPathType;

        private final List<PathRecordType> resolvedPathPatternTypes = new ArrayList<>();

        public GQLPathPatternScope getPathPatternScope() {
            return pathPatternScope;
        }

        public void setPathPatternScope(GQLPathPatternScope pathPatternScope) {
            this.pathPatternScope = pathPatternScope;
        }

        public boolean isFirstNode() {
            return isFirstNode;
        }

        public void setFirstNode(boolean firstNode) {
            isFirstNode = firstNode;
        }

        public PathRecordType getInputPathType() {
            return inputPathType;
        }

        public void setInputPathType(PathRecordType inputPathType) {
            this.inputPathType = inputPathType;
        }

        public void addResolvedPathPatternType(PathRecordType pathRecordType) {
            resolvedPathPatternTypes.add(pathRecordType);
        }

        public RelDataTypeField getResolvedField(String name, boolean caseSensitive) {
            for (PathRecordType pathRecordType : resolvedPathPatternTypes) {
                RelDataTypeField field = pathRecordType.getField(name, caseSensitive, false);
                if (field != null) {
                    return field;
                }
            }
            return null;
        }
    }
}
