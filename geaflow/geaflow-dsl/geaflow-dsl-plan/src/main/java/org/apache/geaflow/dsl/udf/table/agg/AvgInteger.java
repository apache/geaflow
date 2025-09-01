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
import org.apache.geaflow.dsl.udf.table.agg.AvgInteger.Accumulator;

@Description(name = "avg", description = "The avg function for int input.")
public class AvgInteger extends UDAF<Integer, Accumulator, Double> {

    @Override
    public Accumulator createAccumulator() {
        return new Accumulator();
    }

    @Override
    public void accumulate(Accumulator accumulator, Integer input) {
        if (null != input) {
            accumulator.sum += input;
            accumulator.count++;
        }
    }

    @Override
    public void merge(Accumulator merged, Iterable<Accumulator> accumulators) {
        for (Accumulator accumulator : accumulators) {
            merged.sum += accumulator.sum;
            merged.count += accumulator.count;
        }
    }

    @Override
    public void resetAccumulator(Accumulator accumulator) {
        accumulator.sum = 0.0;
        accumulator.count = 0L;
    }

    @Override
    public Double getValue(Accumulator accumulator) {
        return accumulator.count == 0 ? null : (accumulator.sum / (double) accumulator.count);
    }

    public static class Accumulator implements Serializable {
        public double sum = 0.0;
        public long count = 0;
    }
}
