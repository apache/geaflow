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

package org.apache.geaflow.ai.retrieval.validation;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/** Shared constructor validation and defensive-copy helpers for retrieval models. */
public final class ModelValidation {

    private ModelValidation() {
    }

    public static String required(String value, String name) {
        Objects.requireNonNull(value, name);
        if (value.trim().isEmpty()) {
            throw new RetrievalModelValidationException(name + " must not be blank");
        }
        return value;
    }

    public static String optional(String value) {
        return value == null ? null : value;
    }

    public static String optionalNonBlank(String value, String name) {
        if (value == null) {
            return null;
        }
        return required(value, name);
    }

    public static int nonNegative(int value, String name) {
        if (value < 0) {
            throw new RetrievalModelValidationException(name + " must be non-negative");
        }
        return value;
    }

    public static double finite(double value, String name) {
        if (Double.isNaN(value) || Double.isInfinite(value)) {
            throw new RetrievalModelValidationException(name + " must be finite");
        }
        return value;
    }

    public static Double optionalScore(Double value, String name) {
        if (value == null) {
            return null;
        }
        finite(value, name);
        if (value < 0.0 || value > 1.0) {
            throw new RetrievalModelValidationException(name + " must be in [0, 1]");
        }
        return value;
    }

    public static Integer optionalRank(Integer value, String name) {
        if (value != null && value < 1) {
            throw new RetrievalModelValidationException(name + " must be at least 1");
        }
        return value;
    }

    public static <T> List<T> immutableList(List<T> values, String name) {
        if (values == null || values.isEmpty()) {
            return Collections.emptyList();
        }
        List<T> copy = new ArrayList<>(values);
        for (T value : copy) {
            Objects.requireNonNull(value, name + " element");
        }
        return Collections.unmodifiableList(copy);
    }

    public static List<String> sortedStrings(List<String> values, String name) {
        List<String> result = immutableList(values, name);
        if (result.isEmpty()) {
            return result;
        }
        List<String> sorted = new ArrayList<>(result.size());
        for (String value : result) {
            sorted.add(required(value, name));
        }
        sorted.sort(Comparator.naturalOrder());
        return Collections.unmodifiableList(sorted);
    }

    public static <T> Map<String, T> sortedMap(Map<String, T> values) {
        if (values == null || values.isEmpty()) {
            return Collections.emptyMap();
        }
        List<String> keys = new ArrayList<>(values.keySet());
        for (String key : keys) {
            required(key, "map key");
        }
        keys.sort(Comparator.naturalOrder());
        Map<String, T> sorted = new LinkedHashMap<>();
        for (String key : keys) {
            sorted.put(key, values.get(key));
        }
        return Collections.unmodifiableMap(sorted);
    }
}
