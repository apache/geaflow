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

package org.apache.geaflow.dsl.udf.table.other;

import org.apache.geaflow.dsl.common.function.Description;
import org.apache.geaflow.dsl.common.function.UDF;

@Description(name = "if", description = "Return true value if condition is true, else return false value.")
public class If extends UDF {

    public Boolean eval(Boolean condition, Boolean trueValue, Boolean falseValue) {
        if (condition != null && condition) {
            return trueValue;
        }
        return falseValue;
    }

    public Integer eval(Boolean condition, Integer trueValue, Integer falseValue) {
        if (condition != null && condition) {
            return trueValue;
        }
        return falseValue;
    }

    public Double eval(Boolean condition, Double trueValue, Double falseValue) {
        if (condition != null && condition) {
            return trueValue;
        }
        return falseValue;
    }

    public Long eval(Boolean condition, Long trueValue, Long falseValue) {
        if (condition != null && condition) {
            return trueValue;
        }
        return falseValue;
    }

    public String eval(Boolean condition, String trueValue, String falseValue) {
        if (condition != null && condition) {
            return trueValue;
        }
        return falseValue;
    }

    public Object eval(Boolean condition, Object trueValue, Object falseValue) {
        if (condition != null && condition) {
            return trueValue;
        }
        return falseValue;
    }
}
