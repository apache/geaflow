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

import org.apache.geaflow.dsl.common.function.Description;
import org.apache.geaflow.dsl.common.function.UDF;

@Description(name = "sign", description = "Returns the sign of the given value.")
public class Sign extends UDF {

    public Double eval(Double a) {
        if (a == null) {
            return null;
        }
        return Math.signum(a);
    }

    public Long eval(Long n) {
        if (n == null) {
            return null;
        }
        if (n > 0) {
            return 1L;
        } else if (n < 0) {
            return -1L;
        } else {
            return 0L;
        }
    }

    public Integer eval(Integer n) {
        if (n == null) {
            return null;
        }
        if (n > 0) {
            return 1;
        } else if (n < 0) {
            return -1;
        } else {
            return 0;
        }
    }
}
