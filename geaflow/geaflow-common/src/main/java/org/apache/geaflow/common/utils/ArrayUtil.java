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

package org.apache.geaflow.common.utils;

import com.google.common.collect.Sets;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Set;

public class ArrayUtil {

    public static int[] toIntArray(Collection<Integer> list) {
        if (list == null) {
            return null;
        }
        int[] intArray = new int[list.size()];
        int i = 0;
        for (Integer e : list) {
            intArray[i++] = e;
        }
        return intArray;
    }

    public static long[] toLongArray(Collection<Long> list) {
        if (list == null) {
            return null;
        }
        long[] longArray = new long[list.size()];
        int i = 0;
        for (Long e : list) {
            longArray[i++] = e;
        }
        return longArray;
    }

    public static List<Integer> toList(int[] array) {
        if (array == null) {
            return null;
        }
        List<Integer> list = new ArrayList<>();
        for (int i : array) {
            list.add(i);
        }
        return list;
    }

    public static int indexOf(long[] longs, long value) {
        if (longs == null) {
            return -1;
        }
        int index = -1;
        for (int i = 0; i < longs.length; i++) {
            if (longs[i] == value) {
                index = i;
                break;
            }
        }
        return index;
    }

    public static long[] grow(long[] longs, int growSize) {
        long[] newArray;
        if (longs == null) {
            newArray = new long[growSize];
        } else {
            newArray = new long[longs.length + growSize];
            System.arraycopy(longs, 0, newArray, 0, longs.length);
        }
        return newArray;
    }

    public static long[] copy(long[] longs) {
        if (longs == null) {
            return null;
        }
        return Arrays.copyOf(longs, longs.length);
    }

    @SuppressWarnings("unchecked")
    public static <T> T[] concat(T[] a, T[] b) {
        if (a == null) {
            return b;
        }
        if (b == null) {
            return a;
        }
        T[] c = (T[]) Array.newInstance(a.getClass().getComponentType(), a.length + b.length);
        System.arraycopy(a, 0, c, 0, a.length);
        System.arraycopy(b, 0, c, a.length, b.length);
        return c;
    }

    public static <IN, OUT> List<OUT> castList(List<IN> list) {
        if (list == null) {
            return null;
        }
        List<OUT> outList = new ArrayList<>(list.size());
        for (IN in : list) {
            outList.add((OUT) in);
        }
        return outList;
    }

    public static <E> boolean isEmpty(Collection<E> collection) {
        return collection == null || collection.isEmpty();
    }

    public static boolean isEmpty(int[] array) {
        return array == null || array.length == 0;
    }

    public static <T> Set<T> copySet(Set<T> set) {
        if (set == null) {
            return null;
        }
        return Sets.newHashSet(set);
    }

    public static Object[] concatArray(Object[] array1, Object[] array2) {
        if (array1 == null) {
            return array2;
        }
        if (array2 == null) {
            return array1;
        }
        Object[] concat = new Object[array1.length + array2.length];
        System.arraycopy(array1, 0, concat, 0, array1.length);
        System.arraycopy(array2, 0, concat, array1.length, array2.length);
        return concat;
    }
}
