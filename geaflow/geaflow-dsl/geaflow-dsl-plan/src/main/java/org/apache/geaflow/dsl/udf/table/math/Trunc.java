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

@Description(name = "trunc", description = "Truncates x to d decimal places without rounding.")
public class Trunc extends UDF {

    private Double truncate(Double n, int scale) {
        if (Double.isNaN(n) || Double.isInfinite(n)) {
            return n;
        }
        return BigDecimal.valueOf(n).setScale(scale, RoundingMode.DOWN).doubleValue();
    }

    public Double eval(Double n) {
        if (n == null) {
            return null;
        }
        return truncate(n, 0);
    }

    public Long eval(Long n) {
        return n;
    }

    public Integer eval(Integer n) {
        return n;
    }

    public Double eval(Double n, Integer scale) {
        if (n == null || scale == null) {
            return null;
        }
        return truncate(n, scale);
    }

    public Double eval(Double n, Long scale) {
        if (n == null || scale == null) {
            return null;
        }
        return truncate(n, scale.intValue());
    }
}
