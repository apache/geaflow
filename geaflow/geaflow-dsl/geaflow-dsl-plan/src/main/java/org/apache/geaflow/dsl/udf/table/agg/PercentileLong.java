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

import java.util.ArrayList;
import java.util.List;
import org.apache.commons.math3.stat.StatUtils;
import org.apache.geaflow.dsl.common.function.Description;
import org.apache.geaflow.dsl.common.function.UDAF;
import org.apache.geaflow.dsl.common.function.UDAFArguments;
import org.apache.geaflow.dsl.udf.table.agg.PercentileDouble.Accumulator;
import org.apache.geaflow.dsl.udf.table.agg.PercentileLong.MultiArguments;

@Description(name = "percentile", description = "percentile agg function for long")
public class PercentileLong extends UDAF<MultiArguments, Accumulator, Double> {

    @Override
    public Accumulator createAccumulator() {
        return new Accumulator();
    }

    @Override
    public void accumulate(Accumulator accumulator, MultiArguments input) {
        if (input != null) {
            accumulator.setPercent(PercentileDouble.getPercent(input.getParam(1)));
            accumulator.getValueList().add((double) (long) input.getParam(0));
        }
    }

    @Override
    public void merge(Accumulator accumulator, Iterable<Accumulator> its) {
        for (Accumulator it : its) {
            if (it != null) {
                accumulator.getValueList().addAll(it.getValueList());
                accumulator.setPercent(it.getPercent());
            }
        }
    }

    @Override
    public void resetAccumulator(Accumulator accumulator) {
        accumulator.setValueList(new ArrayList<>());
    }

    @Override
    public Double getValue(Accumulator accumulator) {
        List<Double> valueList = accumulator.getValueList();
        double[] values = new double[valueList.size()];
        for (int i = 0; i < valueList.size(); i++) {
            values[i] = valueList.get(i);
        }
        return StatUtils.percentile(values, accumulator.getPercent());
    }

    public static class MultiArguments extends UDAFArguments {

        public MultiArguments() {
        }

        @Override
        public List<Class<?>> getParamTypes() {
            List<Class<?>> types = new ArrayList<>();
            types.add(Long.class);
            types.add(Double.class);
            return types;
        }
    }
}

