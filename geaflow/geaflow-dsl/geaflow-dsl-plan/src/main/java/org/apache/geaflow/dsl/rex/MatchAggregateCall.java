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

import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.Objects;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlAggFunction;

public class MatchAggregateCall {
    private final SqlAggFunction aggFunction;

    private final boolean distinct;
    private final boolean approximate;
    public final RelDataType type;
    public final String name;

    private final ImmutableList<RexNode> argList;
    public final int filterArg;
    public final RelCollation collation;

    public MatchAggregateCall(SqlAggFunction aggFunction, boolean distinct,
                              boolean approximate, List<RexNode> argList, int filterArg,
                              RelCollation collation, RelDataType type, String name) {
        this.type = Objects.requireNonNull(type);
        this.name = name;
        this.aggFunction = Objects.requireNonNull(aggFunction);
        this.argList = ImmutableList.copyOf(argList);
        this.filterArg = filterArg;
        this.collation = Objects.requireNonNull(collation);
        this.distinct = distinct;
        this.approximate = approximate;
    }

    public final boolean isDistinct() {
        return distinct;
    }

    public final boolean isApproximate() {
        return approximate;
    }

    public final SqlAggFunction getAggregation() {
        return aggFunction;
    }

    public RelCollation getCollation() {
        return collation;
    }

    public final List<RexNode> getArgList() {
        return argList;
    }

    public final RelDataType getType() {
        return type;
    }

    public String getName() {
        return name;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        MatchAggregateCall that = (MatchAggregateCall) o;
        return distinct == that.distinct && approximate == that.approximate
            && filterArg == that.filterArg && Objects.equals(aggFunction, that.aggFunction)
            && Objects.equals(type, that.type) && Objects.equals(name, that.name) && Objects.equals(
            argList, that.argList) && Objects.equals(collation, that.collation);
    }

    @Override
    public int hashCode() {
        return Objects.hash(aggFunction, distinct, approximate, type, name, argList, filterArg,
            collation);
    }
}
