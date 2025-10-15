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
import org.apache.geaflow.dsl.udf.table.agg.SumDouble.Accumulator;

@Description(name = "sum", description = "The sum function for double.")
public class SumDouble extends UDAF<Double, Accumulator, Double> {

    @Override
    public Accumulator createAccumulator() {
        return new Accumulator(0.0);
    }

    @Override
    public void accumulate(Accumulator accumulator, Double input) {
        if (null != input) {
            accumulator.value += input;
        }
    }

    @Override
    public void merge(Accumulator accumulator, Iterable<Accumulator> its) {
        for (Accumulator toMerge : its) {
            accumulator.value += toMerge.value;
        }
    }

    @Override
    public void resetAccumulator(Accumulator accumulator) {
        accumulator.value = 0.0;
    }

    @Override
    public Double getValue(Accumulator accumulator) {
        return accumulator.value;
    }

    public static class Accumulator implements Serializable {

        public Accumulator() {
        }

        public double value;

        public Accumulator(double value) {
            this.value = value;
        }

    }
}
