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

import java.util.Objects;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBiVisitor;
import org.apache.calcite.rex.RexSlot;
import org.apache.calcite.rex.RexVisitor;

public class RexParameterRef extends RexSlot {

    private final RelDataType inputType;

    public RexParameterRef(int index, RelDataType type, RelDataType inputType) {
        super("$$" + index, index, type);
        this.inputType = Objects.requireNonNull(inputType);
        this.digest = getName();
    }

    @Override
    public <R> R accept(RexVisitor<R> visitor) {
        return visitor.visitOther(this);
    }

    @Override
    public <R, P> R accept(RexBiVisitor<R, P> visitor, P arg) {
        return visitor.visitOther(this, arg);
    }

    @Override
    public boolean equals(Object obj) {
        return this == obj
            || obj instanceof RexParameterRef
            && index == ((RexParameterRef) obj).index;
    }

    @Override
    public int hashCode() {
        return index;
    }

    public RelDataType getInputType() {
        return inputType;
    }
}
