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
import org.apache.geaflow.dsl.common.function.Description;
import org.apache.geaflow.dsl.common.function.UDAF;
import org.apache.geaflow.dsl.udf.table.agg.BoolAnd.Accumulator;

@Description(name = "bool_and", description = "The boolean AND aggregate function.")
public class BoolAnd extends UDAF<Boolean, Accumulator, Boolean> {

    @Override
    public Accumulator createAccumulator() {
        return new Accumulator(null);
    }

    @Override
    public void accumulate(Accumulator accumulator, Boolean input) {
        if (input != null) {
            accumulator.value = accumulator.value == null ? input : accumulator.value && input;
        }
    }

    @Override
    public void merge(Accumulator accumulator, Iterable<Accumulator> its) {
        for (Accumulator toMerge : its) {
            if (toMerge.value != null) {
                accumulate(accumulator, toMerge.value);
            }
        }
    }

    @Override
    public void resetAccumulator(Accumulator accumulator) {
        accumulator.value = null;
    }

    @Override
    public Boolean getValue(Accumulator accumulator) {
        return accumulator.value;
    }

    public static class Accumulator implements Serializable {

        public Accumulator() {
        }

        public Boolean value;

        public Accumulator(Boolean value) {
            this.value = value;
        }
    }
}
