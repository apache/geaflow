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
import org.apache.geaflow.dsl.udf.table.agg.StdDevSampDouble.Accumulator;

@Description(name = "stddev_samp", description = "The stddev function for double.")
public class StdDevSampDouble extends UDAF<Double, Accumulator, Double> {

    @Override
    public Accumulator createAccumulator() {
        return new Accumulator();
    }

    @Override
    public void accumulate(Accumulator accumulator, Double input) {
        if (null != input) {
            accumulator.add(input);
        }
    }

    @Override
    public void merge(Accumulator merged, Iterable<Accumulator> accumulators) {
        for (Accumulator accumulator : accumulators) {
            merged.squareSum += accumulator.squareSum;
            merged.sum += accumulator.sum;
            merged.count += accumulator.count;
        }
    }

    @Override
    public void resetAccumulator(Accumulator accumulator) {
        accumulator.squareSum = 0.0;
        accumulator.sum = 0.0;
        accumulator.count = 0L;
    }

    @Override
    public Double getValue(Accumulator accumulator) {
        if (accumulator.count == 0) {
            return 0.0;
        }
        return Math.sqrt((accumulator.squareSum - Math.pow(accumulator.sum, 2) / accumulator.count) / accumulator.count);
    }

    public static class Accumulator implements Serializable {

        double squareSum = 0.0;
        double sum = 0.0;
        long count = 0;

        public Accumulator() {
        }

        public void add(double input) {
            sum += input;
            squareSum += input * input;
            count++;
        }
    }
}
