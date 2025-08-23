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

package org.apache.geaflow.dsl.common.function;

/**
 * Interface for the User Defined Aggregate Function.
 */
public abstract class UDAF<InputT, AccumT, OutputT> extends UserDefinedFunction {

    /**
     * Create aggregate accumulator for aggregate function to store the aggregate value.
     */
    public abstract AccumT createAccumulator();

    /**
     * Accumulate the input to the accumulator.
     */
    public abstract void accumulate(AccumT accumulator, InputT input);

    /**
     * Merge the accumulator iterator to the accumulator.
     *
     * @param accumulator The accumulator to merged to.
     * @param its         The accumulator iterators to merge from.
     */
    public abstract void merge(AccumT accumulator, Iterable<AccumT> its);

    /**
     * Reset the accumulator to init value.
     */
    public abstract void resetAccumulator(AccumT accumulator);

    /**
     * Get aggregate function result from the accumulator.
     */
    public abstract OutputT getValue(AccumT accumulator);

}
