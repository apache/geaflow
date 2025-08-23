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

package org.apache.geaflow.dsl.udf.table.math;

import java.math.BigDecimal;
import java.math.RoundingMode;
import org.apache.geaflow.dsl.common.function.Description;
import org.apache.geaflow.dsl.common.function.UDF;

@Description(name = "round", description = "round x to d decimal places")
public class Round extends UDF {

    private Double eval(Double n, int i) {
        double d = n;
        if (Double.isNaN(d) || Double.isInfinite(d)) {
            return d;
        } else {
            return BigDecimal.valueOf(d).setScale(i, RoundingMode.HALF_UP).doubleValue();
        }
    }

    public Double eval(Double n) {
        if (n == null) {
            return null;
        }
        return eval(n, 0);
    }

    public Long eval(Long n) {
        return n;
    }

    public Integer eval(Integer n) {
        return n;
    }

    public Double eval(Double n, Long i) {
        if ((n == null) || (i == null)) {
            return null;
        }
        return eval(n, i.intValue());
    }

    public Double eval(Double n, Integer i) {
        if ((n == null) || (i == null)) {
            return null;
        }
        return eval(n, i.intValue());
    }
}
