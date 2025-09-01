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

package org.apache.geaflow.dsl.rex;

import com.google.common.collect.Lists;
import java.util.List;
import javax.annotation.Nonnull;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexSubQuery;
import org.apache.geaflow.dsl.operator.SqlLambdaOperator;
import org.apache.geaflow.dsl.rel.GraphMatch.ExplainVisitor;
import org.apache.geaflow.dsl.rel.match.IMatchNode;

public class RexLambdaCall extends RexCall {

    public RexLambdaCall(RexSubQuery input, RexNode value) {
        super(value.getType(), SqlLambdaOperator.INSTANCE, Lists.newArrayList(input, value));
    }

    public RexSubQuery getInput() {
        return (RexSubQuery) operands.get(0);
    }

    public RexNode getValue() {
        return operands.get(1);
    }

    @Nonnull
    @Override
    protected String computeDigest(boolean withType) {
        RexSubQuery input = getInput();
        String inputStr;
        if (input.rel instanceof IMatchNode) {
            IMatchNode matchNode = (IMatchNode) input.rel;
            ExplainVisitor explainVisitor = new ExplainVisitor();
            inputStr = explainVisitor.visit(matchNode);
        } else {
            inputStr = input.toString();
        }
        return inputStr + " => " + getValue().toString();
    }

    @Override
    public RexCall clone(RelDataType type, List<RexNode> operands) {
        assert operands.size() == 2;
        return new RexLambdaCall((RexSubQuery) operands.get(0), operands.get(1));
    }
}
