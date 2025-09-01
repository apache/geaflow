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

package org.apache.geaflow.dsl.runtime.traversal.data;

import java.util.Arrays;
import java.util.Objects;
import org.apache.geaflow.dsl.common.data.Row;
import org.apache.geaflow.dsl.common.data.RowEdge;
import org.apache.geaflow.dsl.common.data.impl.types.LongEdge;
import org.apache.geaflow.model.graph.edge.IEdge;

public class FieldAlignLongEdge extends FieldAlignEdge implements RowEdge {

    private final LongEdge baseEdge;

    private final int[] fieldMapping;

    public FieldAlignLongEdge(LongEdge baseEdge, int[] fieldMapping) {
        super(baseEdge, fieldMapping);
        this.baseEdge = baseEdge;
        this.fieldMapping = fieldMapping;
    }

    @Override
    public IEdge<Object, Row> withValue(Row value) {
        return new FieldAlignLongEdge(baseEdge.withValue(value), fieldMapping);
    }

    @Override
    public IEdge<Object, Row> reverse() {
        return new FieldAlignLongEdge(baseEdge.reverse(), fieldMapping);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof RowEdge)) {
            return false;
        }
        if (o instanceof LongEdge) {
            LongEdge that = (LongEdge) o;
            return baseEdge.srcId == that.srcId && baseEdge.targetId == that.targetId
                && baseEdge.direction == that.direction
                && Objects.equals(baseEdge.getBinaryLabel(), that.getBinaryLabel());
        } else if (o instanceof FieldAlignLongEdge) {
            FieldAlignLongEdge that = (FieldAlignLongEdge) o;
            return baseEdge.srcId == that.baseEdge.srcId && baseEdge.targetId == that.baseEdge.targetId
                && baseEdge.direction == that.baseEdge.direction
                && Objects.equals(baseEdge.getBinaryLabel(), that.getBinaryLabel());
        } else {
            RowEdge that = (RowEdge) o;
            return Objects.equals(getSrcId(), that.getSrcId()) && Objects.equals(getTargetId(),
                that.getTargetId()) && getDirect() == that.getDirect() && Objects.equals(getBinaryLabel(), that.getBinaryLabel());
        }
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(baseEdge);
        result = 31 * result + Arrays.hashCode(fieldMapping);
        return result;
    }
}
