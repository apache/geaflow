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

package org.apache.geaflow.infer.exchange.serialize;

import java.io.Serializable;

public class ComplexNumber implements Serializable {

    private static final long serialVersionUID = 4668080260997226513L;
    private static final String ADD_FLAG = "+";
    private static final String I_FLAG = "i";

    private final double r;
    private final double i;

    public ComplexNumber(double rr, double ii) {
        r = rr;
        i = ii;
    }

    public ComplexNumber(Double rr, Double ii) {
        r = rr;
        i = ii;
    }

    public String toString() {
        StringBuffer sb = new StringBuffer().append(r);
        if (i >= 0) {
            sb.append(ADD_FLAG);
        }
        return sb.append(i).append(I_FLAG).toString();
    }

    public double getReal() {
        return r;
    }

    public double getImaginary() {
        return i;
    }

    public double magnitude() {
        return Math.sqrt(r * r + i * i);
    }

    public ComplexNumber add(ComplexNumber other) {
        return add(this, other);
    }

    public static ComplexNumber add(ComplexNumber c1, ComplexNumber c2) {
        return new ComplexNumber(c1.r + c2.r, c1.i + c2.i);
    }

    public ComplexNumber subtract(ComplexNumber other) {
        return subtract(this, other);
    }

    public static ComplexNumber subtract(ComplexNumber c1, ComplexNumber c2) {
        return new ComplexNumber(c1.r - c2.r, c1.i - c2.i);
    }

    public ComplexNumber multiply(ComplexNumber other) {
        return multiply(this, other);
    }

    public static ComplexNumber multiply(ComplexNumber c1, ComplexNumber c2) {
        return new ComplexNumber(c1.r * c2.r - c1.i * c2.i, c1.r * c2.i + c1.i * c2.r);
    }

    public static ComplexNumber divide(ComplexNumber c1, ComplexNumber c2) {
        double value = c2.r * c2.r + c2.i * c2.i;
        return new ComplexNumber((c1.r * c2.r + c1.i * c2.i) / (value),
            (c1.i * c2.r - c1.r * c2.i) / (value));
    }

    public boolean equals(Object o) {
        if (!(o instanceof ComplexNumber)) {
            return false;
        }
        ComplexNumber other = (ComplexNumber) o;
        return r == other.r && i == other.i;
    }

    public int hashCode() {
        return (Double.valueOf(r).hashCode()) ^ (Double.valueOf(i).hashCode());
    }
}
