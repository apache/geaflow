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

package org.apache.geaflow.dsl.udf.table.agg;

import java.io.Serializable;
import org.apache.geaflow.common.binary.BinaryString;
import org.apache.geaflow.dsl.common.function.Description;
import org.apache.geaflow.dsl.common.function.UDAF;
import org.apache.geaflow.dsl.udf.table.agg.MaxBinaryString.Accumulator;

@Description(name = "max", description = "The max function for string.")
public class MaxBinaryString extends UDAF<BinaryString, Accumulator, BinaryString> {
    @Override
    public Accumulator createAccumulator() {
        return new Accumulator(null);
    }

    @Override
    public void accumulate(Accumulator accumulator, BinaryString input) {
        if (null == accumulator.value
            || (null != input && accumulator.value.compareTo(input) < 0)) {
            accumulator.value = input;
        }
    }

    @Override
    public void merge(Accumulator accumulator, Iterable<Accumulator> its) {
        for (Accumulator toMerge : its) {
            if (accumulator.value == null
                || (null != toMerge.value && accumulator.value.compareTo(toMerge.value) < 0)) {
                accumulator.value = toMerge.value;
            }
        }
    }

    @Override
    public void resetAccumulator(Accumulator accumulator) {
        accumulator.value = null;
    }

    @Override
    public BinaryString getValue(Accumulator accumulator) {
        return accumulator.value;
    }

    public static class Accumulator implements Serializable {
        public Accumulator() {
        }

        public BinaryString value;

        public Accumulator(BinaryString value) {
            this.value = value;
        }
    }
}
