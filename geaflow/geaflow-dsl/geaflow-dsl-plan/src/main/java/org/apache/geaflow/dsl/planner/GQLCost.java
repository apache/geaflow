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

package org.apache.geaflow.dsl.planner;

import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptUtil;

public class GQLCost implements RelOptCost {

    static final GQLCost
        INFINITY =
        new GQLCost(Double.POSITIVE_INFINITY, Double.POSITIVE_INFINITY,
            Double.POSITIVE_INFINITY) {
            public String toString() {
                return "{inf}";
            }
        };

    static final GQLCost
        HUGE =
        new GQLCost(Double.MAX_VALUE, Double.MAX_VALUE, Double.MAX_VALUE) {
            public String toString() {
                return "{huge}";
            }
        };

    static final GQLCost ZERO = new GQLCost(0.0, 0.0, 0.0) {
        public String toString() {
            return "{0}";
        }
    };

    static final GQLCost TINY = new GQLCost(1.0, 1.0, 0.0) {
        public String toString() {
            return "{tiny}";
        }
    };

    //~ Instance fields --------------------------------------------------------
    final double cpu;
    final double io;
    final double rowCount;

    //~ Constructors -----------------------------------------------------------
    GQLCost(double rowCount, double cpu, double io) {
        this.rowCount = rowCount;
        this.cpu = cpu;
        this.io = io;
    }

    //~ Methods ----------------------------------------------------------------

    public double getCpu() {
        return cpu;
    }

    public boolean isInfinite() {
        return (this == INFINITY) || (this.rowCount == Double.POSITIVE_INFINITY) || (this.cpu
            == Double.POSITIVE_INFINITY)
            || (this.io == Double.POSITIVE_INFINITY);
    }

    @Override
    public boolean equals(RelOptCost other) {
        return this == other || other instanceof GQLCost && (this.rowCount
            == ((GQLCost) other).rowCount)
            && (this.cpu == ((GQLCost) other).cpu) && (this.io == ((GQLCost) other).io);
    }

    public double getIo() {
        return io;
    }

    public boolean isLe(RelOptCost other) {
        GQLCost that = (GQLCost) other;
        return this == that || this.rowCount <= that.rowCount;
    }

    public boolean isLt(RelOptCost other) {
        GQLCost that = (GQLCost) other;
        return this.rowCount < that.rowCount;
    }

    public double getRows() {
        return rowCount;
    }

    public boolean isEqWithEpsilon(RelOptCost other) {
        if (!(other instanceof GQLCost)) {
            return false;
        }
        GQLCost that = (GQLCost) other;
        return (this == that) || ((Math.abs(this.rowCount - that.rowCount) < RelOptUtil.EPSILON)
            && (Math.abs(this.cpu - that.cpu) < RelOptUtil.EPSILON) && (Math.abs(this.io - that.io)
            < RelOptUtil.EPSILON));
    }

    public RelOptCost minus(RelOptCost other) {
        if (this == INFINITY) {
            return this;
        }
        GQLCost that = (GQLCost) other;
        return new GQLCost(this.rowCount - that.rowCount, this.cpu - that.cpu,
            this.io - that.io);
    }

    public RelOptCost multiplyBy(double factor) {
        if (this == INFINITY) {
            return this;
        }
        return new GQLCost(rowCount * factor, cpu * factor, io * factor);
    }

    public double divideBy(RelOptCost cost) {
        // Compute the geometric average create the ratios create all create the factors
        // which are non-zero and finite.
        GQLCost that = (GQLCost) cost;
        double d = 1;
        double n = 0;
        if ((this.rowCount != 0) && !Double.isInfinite(this.rowCount) && (that.rowCount != 0)
            && !Double.isInfinite(that.rowCount)) {
            d *= this.rowCount / that.rowCount;
            ++n;
        }
        if ((this.cpu != 0) && !Double.isInfinite(this.cpu) && (that.cpu != 0) && !Double
            .isInfinite(that.cpu)) {
            d *= this.cpu / that.cpu;
            ++n;
        }
        if ((this.io != 0) && !Double.isInfinite(this.io) && (that.io != 0) && !Double
            .isInfinite(that.io)) {
            d *= this.io / that.io;
            ++n;
        }
        if (n == 0) {
            return 1.0;
        }
        return Math.pow(d, 1 / n);
    }

    public RelOptCost plus(RelOptCost other) {
        GQLCost that = (GQLCost) other;
        if ((this == INFINITY) || (that == INFINITY)) {
            return INFINITY;
        }
        return new GQLCost(this.rowCount + that.rowCount, this.cpu + that.cpu,
            this.io + that.io);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        GQLCost that = (GQLCost) o;

        if (Double.compare(that.cpu, cpu) != 0) {
            return false;
        }
        if (Double.compare(that.io, io) != 0) {
            return false;
        }
        return Double.compare(that.rowCount, rowCount) == 0;
    }

    @Override
    public int hashCode() {
        int result;
        long temp;
        temp = Double.doubleToLongBits(cpu);
        result = (int) (temp ^ (temp >>> 32));
        temp = Double.doubleToLongBits(io);
        result = 31 * result + (int) (temp ^ (temp >>> 32));
        temp = Double.doubleToLongBits(rowCount);
        result = 31 * result + (int) (temp ^ (temp >>> 32));
        return result;
    }

    @Override
    public String toString() {
        return "{" + rowCount + " rows, " + cpu + " cpu, " + io + " io}";
    }
}
