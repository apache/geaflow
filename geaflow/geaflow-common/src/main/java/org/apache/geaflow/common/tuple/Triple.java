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

package org.apache.geaflow.common.tuple;

import java.io.Serializable;
import java.util.Objects;
import org.apache.geaflow.common.utils.StringUtils;

public class Triple<F0, F1, F2> implements Serializable {

    public F0 f0;

    public F1 f1;

    public F2 f2;

    public Triple(F0 f0, F1 f1, F2 f2) {
        this.f0 = f0;
        this.f1 = f1;
        this.f2 = f2;
    }

    public static <F0, F1, F2> Triple<F0, F1, F2> of(F0 f0, F1 f1, F2 f2) {
        return new Triple<>(f0, f1, f2);
    }

    public F0 getF0() {
        return f0;
    }

    public void setF0(F0 f0) {
        this.f0 = f0;
    }

    public F1 getF1() {
        return f1;
    }

    public void setF1(F1 f1) {
        this.f1 = f1;
    }

    public F2 getF2() {
        return f2;
    }

    public void setF2(F2 f2) {
        this.f2 = f2;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof Triple)) {
            return false;
        }
        Triple<?, ?, ?> tuple3 = (Triple<?, ?, ?>) o;
        return Objects.equals(f0, tuple3.f0) && Objects.equals(f1, tuple3.f1)
            && Objects.equals(f2, tuple3.f2);
    }

    @Override
    public int hashCode() {
        return Objects.hash(f0, f1, f2);
    }

    @Override
    public String toString() {
        return "(" + StringUtils.arrayAwareToString(f0)
            + "," + StringUtils.arrayAwareToString(f1)
            + "," + StringUtils.arrayAwareToString(f2) + ")";
    }
}
