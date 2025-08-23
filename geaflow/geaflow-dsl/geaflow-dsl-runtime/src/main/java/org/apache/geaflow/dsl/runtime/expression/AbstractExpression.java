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

package org.apache.geaflow.dsl.runtime.expression;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.apache.geaflow.dsl.runtime.expression.field.PathFieldExpression;
import org.apache.geaflow.dsl.runtime.expression.logic.AndExpression;

public abstract class AbstractExpression implements Expression {

    @Override
    public String toString() {
        return showExpression();
    }

    @Override
    public int hashCode() {
        return toString().hashCode();
    }

    @Override
    public boolean equals(Object that) {
        if (!(that instanceof Expression)) {
            return false;
        }
        return toString().equals(that.toString());
    }

    @Override
    public List<Integer> getRefPathFieldIndices() {
        List<Integer> pathFields = new ArrayList<>();
        getInputs().forEach(input -> pathFields.addAll(input.getRefPathFieldIndices()));
        if (this instanceof PathFieldExpression) {
            pathFields.add(((PathFieldExpression) this).getFieldIndex());
        }
        return pathFields;
    }

    @Override
    public Expression replace(Function<Expression, Expression> replaceFn) {
        List<Expression> newInputs = getInputs().stream()
            .map(input -> input.replace(replaceFn))
            .collect(Collectors.toList());
        Expression replaceExpression = replaceFn.apply(this);
        if (replaceExpression != this) {
            return replaceExpression.copy(newInputs);
        }
        return this.copy(newInputs);
    }

    @Override
    public List<Expression> collect(Predicate<Expression> condition) {
        List<Expression> collects = getInputs().stream()
            .flatMap(input -> input.collect(condition).stream())
            .collect(Collectors.toList());
        if (condition.test(this)) {
            collects.add(this);
        }
        return collects;
    }

    @Override
    public List<Expression> splitByAnd() {
        if (this instanceof AndExpression) {
            AndExpression and = (AndExpression) this;
            return and.getInputs().stream()
                .flatMap(input -> input.splitByAnd().stream())
                .collect(Collectors.toList());
        }
        return Collections.singletonList(this);
    }
}
