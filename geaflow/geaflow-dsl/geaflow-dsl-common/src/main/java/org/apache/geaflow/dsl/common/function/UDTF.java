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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import java.util.List;
import org.apache.geaflow.dsl.common.exception.GeaFlowDSLException;

public abstract class UDTF extends UserDefinedFunction {

    protected List<Object[]> collector;

    public UDTF() {
        this.collector = Lists.newArrayList();
    }

    /**
     * Collect the result.
     */
    protected void collect(Object[] output) {
        if (output == null) {
            throw new GeaFlowDSLException("UDTF's output must not null, "
                + "Please check your UDTF's logic");
        }
        this.collector.add(output);
    }

    public List<Object[]> getCollectData() {
        ImmutableList values = ImmutableList.copyOf(collector);
        collector.clear();
        return values;
    }

    /**
     * Returns type output types for the function.
     *
     * @param paramTypes    The parameter types of the function.
     * @param outFieldNames The output fields of the function in the sql.
     */
    public abstract List<Class<?>> getReturnType(List<Class<?>> paramTypes, List<String> outFieldNames);
}
