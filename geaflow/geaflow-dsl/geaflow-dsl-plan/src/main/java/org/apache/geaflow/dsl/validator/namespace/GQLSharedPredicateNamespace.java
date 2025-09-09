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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.sql.validate.SqlValidatorImpl;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.geaflow.dsl.calcite.PathRecordType;
import org.apache.geaflow.dsl.calcite.UnionPathRecordType;
import org.apache.geaflow.dsl.common.exception.GeaFlowDSLException;
import org.apache.geaflow.dsl.sqlnode.SqlPathPattern;
import org.apache.geaflow.dsl.sqlnode.SqlSharedPredicatePattern;
import org.apache.geaflow.dsl.validator.namespace.GQLMatchNodeNamespace.MatchNodeContext;

/**
 * Namespace for validating shared predicate patterns in GQL.
 * This namespace handles the validation of shared predicate patterns where
 * two path patterns share a common predicate condition.
 */
public class GQLSharedPredicateNamespace extends GQLBaseNamespace {

    /**
     * The shared predicate pattern being validated
     */
    private final SqlSharedPredicatePattern sharedPredicatePattern;

    /**
     * Context for match node validation
     */
    private MatchNodeContext matchNodeContext;

    /**
     * Constructor for GQLSharedPredicateNamespace
     *
     * @param validator the validator instance
     * @param sharedPredicatePattern the shared predicate pattern to validate
     */
    public GQLSharedPredicateNamespace(SqlValidatorImpl validator, SqlSharedPredicatePattern sharedPredicatePattern) {
        super(validator, sharedPredicatePattern);
        this.sharedPredicatePattern = sharedPredicatePattern;
    }

    /**
     * Set the match node context for validation
     *
     * @param matchNodeContext the match node context
     */
    public void setMatchNodeContext(MatchNodeContext matchNodeContext) {
        this.matchNodeContext = matchNodeContext;
    }

    @Override
    protected RelDataType validateImpl(RelDataType targetRowType) {
        SqlValidatorScope scope = getValidator().getScopes(sharedPredicatePattern);
        List<PathRecordType> pathRecordTypes = new ArrayList<>();

        // Validate left path pattern
        validatePathPattern(sharedPredicatePattern.getLeft(), matchNodeContext, scope, pathRecordTypes);

        // Validate right path pattern
        validatePathPattern(sharedPredicatePattern.getRight(), matchNodeContext, scope, pathRecordTypes);

        // Validate same predicate condition
        validateSharedPredicate(sharedPredicatePattern.getPredicate(), scope, pathRecordTypes);

        // Create union path record type from all path patterns
        PathRecordType patternType = new UnionPathRecordType(pathRecordTypes,
            this.getValidator().getTypeFactory());
        return patternType;
    }

    /**
     * Validate a path pattern and add its type to the list
     *
     * @param pathPatternNode the path pattern node to validate
     * @param matchNodeContext the match node context
     * @param scope the validator scope
     * @param pathPatternTypes list to collect path pattern types
     */
    private void validatePathPattern(SqlNode pathPatternNode,
                                     MatchNodeContext matchNodeContext,
                                     SqlValidatorScope scope,
                                     List<PathRecordType> pathPatternTypes) {
        if (pathPatternNode instanceof SqlPathPattern) {
            SqlPathPattern pathPattern = (SqlPathPattern) pathPatternNode;
            GQLPathPatternNamespace pathPatternNs =
                (GQLPathPatternNamespace) validator.getNamespace(pathPatternNode);
            pathPatternNs.setMatchNodeContext(matchNodeContext);

            // Validate the path pattern
            pathPattern.validate(validator, scope);
            RelDataType pathType = validator.getValidatedNodeType(pathPattern);

            if (!(pathType instanceof PathRecordType)) {
                throw new IllegalStateException("PathPattern should return PathRecordType, but got: " + pathType.getClass());
            }

            // Add resolved path pattern type to context
            matchNodeContext.addResolvedPathPatternType((PathRecordType) pathType);
            pathPatternTypes.add((PathRecordType) pathType);
        } else {
            // Handle other types of path pattern nodes
            pathPatternNode.validate(validator, scope);
            RelDataType pathType = validator.getValidatedNodeType(pathPatternNode);

            if (pathType instanceof PathRecordType) {
                pathPatternTypes.add((PathRecordType) pathType);
            } else {
                throw new GeaFlowDSLException(pathPatternNode.getParserPosition(),
                    "Path pattern node should return PathRecordType, but got: " + pathType.getClass());
            }
        }
    }

    /**
     * Validate the same predicate condition
     *
     * @param predicate the predicate to validate
     * @param scope the validator scope
     * @param pathPatternTypes list of path pattern types
     */
    private void validateSharedPredicate(SqlNode predicate, SqlValidatorScope scope,
                                       List<PathRecordType> pathPatternTypes) {
        // Validate the predicate expression
        predicate.validate(validator, scope);

        // Check that predicate variables exist in all path patterns
        validatePredicateVariableScope(predicate, pathPatternTypes);
    }

    /**
     * Validate that variables referenced in the predicate exist in all path patterns
     * and have compatible types
     *
     * @param predicate the predicate to validate
     * @param pathPatternTypes list of path pattern types
     */
    private void validatePredicateVariableScope(SqlNode predicate, List<PathRecordType> pathPatternTypes) {
        // Collect variables referenced in the predicate
        Set<String> predicateVariables = collectVariableReferences(predicate);

        // Check each variable in all path patterns
        for (String variable : predicateVariables) {
            RelDataType firstType = null;
            boolean foundInAllPatterns = true;

            for (PathRecordType pathType : pathPatternTypes) {
                RelDataTypeField field = pathType.getField(variable, isCaseSensitive(), false);
                if (field == null) {
                    foundInAllPatterns = false;
                    break;
                }

                if (firstType == null) {
                    firstType = field.getType();
                } else if (!SqlTypeUtil.isComparable(firstType, field.getType())) {
                    throw new GeaFlowDSLException(predicate.getParserPosition(),
                        "Variable '{}' has incompatible types across path patterns: {} vs {}",
                        variable, firstType, field.getType());
                }
            }

            if (!foundInAllPatterns) {
                throw new GeaFlowDSLException(predicate.getParserPosition(),
                    "Variable '{}' is not available in all path patterns", variable);
            }
        }
    }

    /**
     * Collect all variable references from a SQL node
     *
     * @param node the SQL node to analyze
     * @return set of variable names
     */
    private Set<String> collectVariableReferences(SqlNode node) {
        Set<String> variables = new HashSet<>();
        collectVariableReferencesRecursive(node, variables);
        return variables;
    }

    /**
     * Recursively collect variable references from a SQL node
     *
     * @param node the SQL node to analyze
     * @param variables set to collect variable names
     */
    private void collectVariableReferencesRecursive(SqlNode node, Set<String> variables) {
        if (node instanceof SqlIdentifier) {
            SqlIdentifier id = (SqlIdentifier) node;
            if (id.names.size() >= 2) {
                // This is a qualified identifier like "a.age"
                variables.add(id.names.get(0));
            }
        } else if (node instanceof SqlCall) {
            SqlCall call = (SqlCall) node;
            for (SqlNode operand : call.getOperandList()) {
                if (operand != null) {
                    collectVariableReferencesRecursive(operand, variables);
                }
            }
        }
    }

    @Override
    public SqlNode getNode() {
        return sharedPredicatePattern;
    }
}
