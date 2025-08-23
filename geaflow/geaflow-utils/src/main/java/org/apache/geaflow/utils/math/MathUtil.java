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

package org.apache.geaflow.utils.math;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class MathUtil {

    public static final long SECOND_IN_MS = 1000L;
    public static final long MINUTE_IN_MS = 60L * SECOND_IN_MS;
    public static final long HOUR_IN_MS = 60L * MINUTE_IN_MS;

    public static final long MINUTE = 60L;
    public static final long HOUR = 60L * MINUTE;

    public static final int MAXIMUM_CAPACITY = 1 << 30;

    private MathUtil() {
    }

    /**
     * Check if the array has deviating elements.
     * <p/>
     * Deviating elements are found by comparing each individual value against the average.
     * @param values the array of values to check
     * @param buffer the amount to ignore as a buffer for smaller valued lists
     * @param factor the amount of allowed deviation is calculated from average * factor
     * @return the index of the deviating value, or -1 if
     */
    public static int[] deviates(long[] values, long buffer, double factor) {
        if (values == null || values.length == 0) {
            return new int[0];
        }

        long avg = average(values);

        // Find deviated elements
        long minimumDiff = Math.max(buffer, (long) (avg * factor));
        List<Integer> deviatedElements = new ArrayList<Integer>();

        for (int i = 0; i < values.length; i++) {
            long diff = values[i] - avg;
            if (diff > minimumDiff) {
                deviatedElements.add(i);
            }
        }

        int[] result = new int[deviatedElements.size()];
        for (int i = 0; i < result.length; i++) {
            result[i] = deviatedElements.get(i);
        }

        return result;
    }

    /**
     * The percentile method returns the least value from the given list which has at least given
     * percentile.
     * @param values The list of values to find the percentile from
     * @param percentile The percentile
     * @return The least value from the list with at least the given percentile
     */
    public static long percentile(List<Long> values, int percentile) {

        if (values.size() == 0) {
            throw new IllegalArgumentException("Percentile of empty list is not defined.");
        }

        if (percentile > 100 || percentile < 0) {
            throw new IllegalArgumentException("Percentile has to be between 0-100");
        }

        if (percentile == 0) {
            return 0;
        }

        Collections.sort(values);

        // Use Nearest Rank method.
        // https://en.wikipedia.org/wiki/Percentile#The_Nearest_Rank_method
        int position = (int) Math.ceil(values.size() * percentile / 100.0);

        // should never happen.
        if (position == 0) {
            return values.get(position);
        }

        // position is always one greater than index. Return value at the proper index
        return values.get(position - 1);
    }

    /**
     * This function hashes an integer value.
     *
     * <p>It is crucial to use different hash functions to partition data across machines and the
     * internal partitioning of data structures. This hash function is intended for partitioning
     * across machines.
     *
     * @param code The integer to be hashed.
     * @return The non-negative hash code for the integer.
     */
    public static int murmurHash(int code) {
        code *= 0xcc9e2d51;
        code = Integer.rotateLeft(code, 15);
        code *= 0x1b873593;

        code = Integer.rotateLeft(code, 13);
        code = code * 5 + 0xe6546b64;

        code ^= 4;
        code = bitMix(code);

        if (code >= 0) {
            return code;
        } else if (code != Integer.MIN_VALUE) {
            return -code;
        } else {
            return 0;
        }
    }


    public static long[][] findTwoGroups(long[] values) {
        return findTwoGroupsRecursive(values, average(values), 2);
    }

    public static long[][] findTwoGroupsRecursive(long[] values, long middle, int levels) {
        if (levels > 0) {
            long[][] result = twoMeans(values, middle);
            long newMiddle = average(result[1]) - average(result[0]);
            return findTwoGroupsRecursive(values, newMiddle, levels - 1);
        }
        return twoMeans(values, middle);
    }

    private static long[][] twoMeans(long[] values, long middle) {
        List<Long> smaller = new ArrayList<Long>();
        List<Long> larger = new ArrayList<Long>();
        for (int i = 0; i < values.length; i++) {
            if (values[i] < middle) {
                smaller.add(values[i]);
            } else {
                larger.add(values[i]);
            }
        }

        long[][] result = new long[2][];
        result[0] = toArray(smaller);
        result[1] = toArray(larger);

        return result;
    }

    private static long[] toArray(List<Long> input) {
        long[] result = new long[input.size()];
        for (int i = 0; i < result.length; i++) {
            result[i] = input.get(i);
        }
        return result;
    }

    /**
     * Compute average for the given array of long.
     * @param values the values
     * @return The average(values)
     */
    public static long average(long[] values) {
        //Find average
        double sum = 0d;
        for (long value : values) {
            sum += value;
        }
        return (long) (sum / (double) values.length);
    }

    /**
     * Compute average for a List of long values.
     * @param values the values
     * @return The average(values)
     */
    public static long average(List<Long> values) {
        //Find average
        double sum = 0d;
        for (long value : values) {
            sum += value;
        }
        return (long) (sum / (double) values.size());
    }

    /**
     * Find the median of the given list.
     * @param values The values
     * @return The median(values)
     */
    public static long median(List<Long> values) {
        if (values.size() == 0) {
            throw new IllegalArgumentException("Median of an empty list is not defined.");
        }
        Collections.sort(values);
        int middle = values.size() / 2;
        if (values.size() % 2 == 0) {
            return (values.get(middle - 1) + values.get(middle)) / 2;
        } else {
            return values.get(middle);
        }
    }

    /**
     * Fast method of finding the next power of 2 greater than or equal to the supplied value.
     *
     * <p/>
     * If the value is {@code <= 0} then 1 will be returned.
     * This method is not suitable for {@link Integer#MIN_VALUE} or numbers greater than 2^30.
     *
     * @param value from which to search for next power of 2
     * @return The next power of 2 or the value itself if it is a power of 2
     */
    public static int findNextPositivePowerOfTwo(final int value) {
        assert value > Integer.MIN_VALUE && value < 0x40000000;
        return 1 << (32 - Integer.numberOfLeadingZeros(value - 1));
    }

    /**
     * Fast method of finding the next power of 2 greater than or equal to the supplied value.
     * <p/>
     * This method will do runtime bounds checking and call {@link #findNextPositivePowerOfTwo(int)} if within a
     * valid range.
     * @param value from which to search for next power of 2
     * @return The next power of 2 or the value itself if it is a power of 2.
     * <p/>
     *      Special cases for return values are as follows:
     * <ul>
     *     <li>{@code <= 0} -> 1</li>
     *     <li>{@code >= 2^30} -> 2^30</li>
     * </ul>
     */
    public static int safeFindNextPositivePowerOfTwo(final int value) {
        return value <= 0 ? 1 : value >= 0x40000000 ? 0x40000000 : findNextPositivePowerOfTwo(value);
    }

    public static boolean isPrime(int n) {
        for (int i = 2; i * i <= n; ++i) {
            if (n % i == 0) {
                return false;
            }
        }
        return true;
    }

    public static int nextPrime(int n) {
        for (int num = n; num < n * 2; num++) {
            if (isPrime(num)) {
                return num;
            }
        }
        return n;
    }

    /**
     * Pseudo-randomly maps a long (64-bit) to an integer (32-bit) using some bit-mixing for better
     * distribution.
     *
     * @param in the long (64-bit)input.
     * @return the bit-mixed int (32-bit) output
     */
    public static int longToIntWithBitMixing(long in) {
        in = (in ^ (in >>> 30)) * 0xbf58476d1ce4e5b9L;
        in = (in ^ (in >>> 27)) * 0x94d049bb133111ebL;
        in = in ^ (in >>> 31);
        return (int) in;
    }

    // ============================================================================================

    /**
     * Bit-mixing for pseudo-randomization of integers (e.g., to guard against bad hash functions).
     * Implementation is from Murmur's 32 bit finalizer.
     *
     * @param in the input value
     * @return the bit-mixed output value
     */
    public static int bitMix(int in) {
        in ^= in >>> 16;
        in *= 0x85ebca6b;
        in ^= in >>> 13;
        in *= 0xc2b2ae35;
        in ^= in >>> 16;
        return in;
    }

    public static int multiplesOf50(int input) {
        return input / 50 * 50;
    }

    /**
     * Check whether input is power of two.
     *
     * @param input
     * @return
     */
    public static boolean isPowerOf2(int input) {
        if (input > 0) {
            return input == 1 || (input & (-input)) == input;
        }
        return false;
    }

    /**
     * Compute the min num n power of two.
     *
     * @param input
     * @return The min n power of two for input.
     */
    public static int minPowerOf2(int input) {
        int n = input - 1;
        n |= n >>> 1;
        n |= n >>> 2;
        n |= n >>> 4;
        n |= n >>> 8;
        n |= n >>> 16;
        return (n < 0) ? 1 : (n >= MAXIMUM_CAPACITY) ? MAXIMUM_CAPACITY : n + 1;
    }

}
