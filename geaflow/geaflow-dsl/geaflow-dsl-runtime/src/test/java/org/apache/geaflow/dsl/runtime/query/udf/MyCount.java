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

package org.apache.geaflow.dsl.runtime.query.udf;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import org.apache.geaflow.common.binary.BinaryString;
import org.apache.geaflow.dsl.common.function.Description;
import org.apache.geaflow.dsl.common.function.UDAF;
import org.apache.geaflow.dsl.common.function.UDAFArguments;
import org.apache.geaflow.dsl.runtime.query.udf.MyCount.Accumulator;
import org.apache.geaflow.dsl.runtime.query.udf.MyCount.MultiArguments;

@Description(name = "count", description = "custom count function for test")
public class MyCount extends UDAF<MultiArguments, Accumulator, Long> {

    @Override
    public Accumulator createAccumulator() {
        return new Accumulator(0);
    }

    @Override
    public void accumulate(Accumulator accumulator, MultiArguments input) {
        if (input.getParam(0).toString().equalsIgnoreCase("jim")) {
            accumulator.value += 10000L;
        }
        accumulator.value += (long) input.getParam(1);
    }

    @Override
    public void merge(Accumulator accumulator, Iterable<Accumulator> its) {
        for (Accumulator toMerge : its) {
            accumulator.value += toMerge.value;
        }
    }

    @Override
    public void resetAccumulator(Accumulator accumulator) {
        accumulator.value = 0L;
    }

    @Override
    public Long getValue(Accumulator accumulator) {
        return accumulator.value;
    }

    public static class Accumulator implements Serializable {

        public Accumulator() {
        }

        public long value = 0;

        public Accumulator(long value) {
            this.value = value;
        }

        @Override
        public String toString() {
            return "Accumulator{"
                + "value=" + value
                + '}';
        }
    }

    public static class MultiArguments extends UDAFArguments {

        public MultiArguments() {
        }

        @Override
        public List<Class<?>> getParamTypes() {
            List<Class<?>> types = new ArrayList<>();
            types.add(BinaryString.class);
            types.add(Long.class);
            return types;
        }
    }
}

