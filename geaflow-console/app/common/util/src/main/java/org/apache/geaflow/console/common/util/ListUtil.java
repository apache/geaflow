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

package org.apache.geaflow.console.common.util;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.commons.collections.CollectionUtils;

@SuppressWarnings("unchecked")
public class ListUtil {

    public static <T> Collection<T> join(Collection<T> left, Collection<T> right) {
        if (left == null || right == null) {
            return null;
        }

        return (Collection<T>) CollectionUtils.intersection(left, right);
    }

    public static <T> Collection<T> union(Collection<T> left, Collection<T> right) {
        if (left == null) {
            return right;
        }

        if (right == null) {
            return left;
        }

        return (List<T>) CollectionUtils.union(left, right);
    }

    public static <T> Map<String, T> toMap(Collection<T> list, Function<T, String> key) {
        if (CollectionUtils.isEmpty(list)) {
            return new HashMap<>();
        }
        return list.stream().collect(Collectors.toMap(key, e -> e));
    }

    public static <T, V> Map<String, V> toMap(Collection<T> list, Function<T, String> key, Function<T, V> value) {
        if (CollectionUtils.isEmpty(list)) {
            return new HashMap<>();
        }
        return list.stream().collect(Collectors.toMap(key, value));
    }

    public static <T, V> List<V> convert(Collection<T> list, Function<T, V> function) {
        if (CollectionUtils.isEmpty(list)) {
            return new ArrayList<>();
        }
        return list.stream().map(function).collect(Collectors.toList());
    }


    public static <T> List<T> diff(List<T> left, List<T> right) {
        return diff(left, right, null);
    }

    public static <T> List<T> diff(List<T> left, List<T> right, Function<T, Object> function) {
        return DiffHelper.diff(left, right, function);
    }

    private static class DiffHelper {

        private static <T> List<T> diff(List<T> left, List<T> right, Function<T, Object> function) {
            if (left == null) {
                return right;
            }

            if (right == null) {
                return left;
            }

            ArrayList<T> list = new ArrayList<>();
            Map<Object, Integer> mapLeft = getCardinalityMap(left, function);
            Map<Object, Integer> mapRight = getCardinalityMap(right, function);
            // duplicate removal  for the left list
            HashMap<Object, T> objMap = new HashMap<>();
            for (T t : left) {
                objMap.putIfAbsent(getKey(t, function), t);
            }
            // calculate the diff of the number of the same key
            for (Entry<Object, T> entry : objMap.entrySet()) {
                Object key = entry.getKey();
                for (int i = 0, m = getFreq(key, mapLeft) - getFreq(key, mapRight); i < m; i++) {
                    list.add(entry.getValue());
                }

            }

            return list;
        }

        private static <T> Object getKey(T obj, Function<T, Object> function) {
            return function == null ? obj : function.apply(obj);
        }

        private static <T> Map<Object, Integer> getCardinalityMap(final Collection coll, Function<T, Object> function) {
            Map<Object, Integer> count = new HashMap();
            for (Iterator<T> it = coll.iterator(); it.hasNext(); ) {
                Object key = getKey(it.next(), function);
                Integer c = (count.get(key));
                if (c == null) {
                    count.put(key, 1);
                } else {
                    count.put(key, c + 1);
                }
            }
            return count;
        }

        private static <T> int getFreq(Object key, final Map<Object, Integer> freqMap) {
            Integer count = freqMap.get(key);
            if (count != null) {
                return count;
            }
            return 0;
        }
    }
}
