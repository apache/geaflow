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

package org.apache.geaflow.ai.index.vector;

import java.util.Objects;

public class MagnitudeVector implements IVector {

    private final double magnitude;

    public MagnitudeVector() {
        this.magnitude = 0.0;
    }

    public MagnitudeVector(double magnitude) {
        this.magnitude = magnitude;
    }

    public double getMagnitude() {
        return magnitude;
    }

    @Override
    public double match(IVector other) {
        if (!(other instanceof MagnitudeVector)) {
            throw new IllegalArgumentException("Other vector must be a MagnitudeVector");
        }

        MagnitudeVector otherVec = (MagnitudeVector) other;
        double otherMagnitude = otherVec.magnitude;

        return computeSimilarity(otherMagnitude);
       
    }

    private double computeSimilarity(double otherMagnitude) {
        if (this.magnitude == 0.0 && otherMagnitude == 0.0) {
            return 1.0;
        }

        if (this.magnitude == 0.0 || otherMagnitude == 0.0) {
            return 0.0;
        }

        double diff = Math.abs(this.magnitude - otherMagnitude);
        double max = Math.max(Math.abs(this.magnitude), Math.abs(otherMagnitude));

        if (max == 0.0) {
            return 1.0;
        }

        return 1.0 - (diff / max);
    }
    
    @Override
    public VectorType getType() {
        return VectorType.MagnitudeVector;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        MagnitudeVector that = (MagnitudeVector) o;
        return Double.compare(that.magnitude, magnitude) == 0;
    }

    @Override
    public int hashCode() {
        return Objects.hash(magnitude);
    }

    @Override
    public String toString() {
        return "MagnitudeVector{magnitude=" + magnitude + '}';
    }
}
