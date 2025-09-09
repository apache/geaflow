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

package org.apache.geaflow.dsl.common.util;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import java.lang.reflect.Array;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.geaflow.common.binary.BinaryString;
import org.apache.geaflow.dsl.common.exception.GeaFlowDSLException;
import org.apache.geaflow.dsl.common.function.UDAF;
import org.apache.geaflow.dsl.common.function.UDAFArguments;

public class FunctionCallUtils {

    public static String UDF_EVAL_METHOD_NAME = "eval";

    // Type degree mappings for implicit type conversion hierarchy
    private static final TypeDegreeMapping[] TYPE_DEGREE_MAPPINGS = {
        new TypeDegreeMapping(Integer.class, new Class<?>[]{Long.class, Double.class, BigDecimal.class}),
        new TypeDegreeMapping(Long.class, new Class<?>[]{Double.class, BigDecimal.class}),
        new TypeDegreeMapping(Byte.class, new Class<?>[]{Integer.class, Long.class, Double.class, BigDecimal.class}),
        new TypeDegreeMapping(Short.class, new Class<?>[]{Integer.class, Long.class, Double.class, BigDecimal.class}),
        new TypeDegreeMapping(BigDecimal.class, new Class<?>[]{Double.class})
    };

    private static final Map<Class<?>, Class<?>[]> TYPE_DEGREE_MAP = new HashMap<>();

    static {
        // Initialize type degree mappings from the definitions
        for (TypeDegreeMapping mapping : TYPE_DEGREE_MAPPINGS) {
            TYPE_DEGREE_MAP.put(mapping.sourceType, mapping.targetTypes);
        }

        // Validate type degree mappings
        validateTypeDegreeMappings();
    }

    /**
     * Validates that type degree mappings are consistent.
     */
    private static void validateTypeDegreeMappings() {
        for (TypeDegreeMapping mapping : TYPE_DEGREE_MAPPINGS) {
            if (mapping.targetTypes == null || mapping.targetTypes.length == 0) {
                throw new IllegalStateException(
                    "Empty target types for source type: " + mapping.sourceType);
            }

            // Check for duplicate target types
            for (int i = 0; i < mapping.targetTypes.length; i++) {
                for (int j = i + 1; j < mapping.targetTypes.length; j++) {
                    if (mapping.targetTypes[i].equals(mapping.targetTypes[j])) {
                        throw new IllegalStateException(
                            "Duplicate target type " + mapping.targetTypes[i]
                            + " in mapping for source type: " + mapping.sourceType);
                    }
                }
            }
        }
    }

    /**
     * Internal class to define type degree mapping for implicit conversion.
     */
    private static class TypeDegreeMapping {
        final Class<?> sourceType;
        final Class<?>[] targetTypes;

        TypeDegreeMapping(Class<?> sourceType, Class<?>[] targetTypes) {
            this.sourceType = sourceType;
            this.targetTypes = targetTypes;
        }
    }

    // Type mapping definitions to reduce duplication.
    private static final TypeMapping[] PRIMITIVE_TYPE_MAPPINGS = {
        new TypeMapping(int.class, Integer.class),
        new TypeMapping(long.class, Long.class),
        new TypeMapping(short.class, Short.class),
        new TypeMapping(byte.class, Byte.class),
        new TypeMapping(boolean.class, Boolean.class),
        new TypeMapping(double.class, Double.class)
    };

    private static final Map<Class<?>, Class<?>> BOX_TYPE_MAPS = new HashMap<>();
    private static final Map<Class<?>, Class<?>> UNBOX_TYPE_MAPS = new HashMap<>();

    static {
        // Initialize type mappings from the definitions
        for (TypeMapping mapping : PRIMITIVE_TYPE_MAPPINGS) {
            BOX_TYPE_MAPS.put(mapping.primitiveType, mapping.wrapperType);
            UNBOX_TYPE_MAPS.put(mapping.wrapperType, mapping.primitiveType);
        }

        // Validate that all mappings are bidirectional
        validateTypeMappings();
    }

    /**
     * Validates that all type mappings are consistent and bidirectional.
     */
    private static void validateTypeMappings() {
        // Ensure all boxed types have corresponding unboxed types
        for (Class<?> primitiveType : BOX_TYPE_MAPS.keySet()) {
            Class<?> wrapperType = BOX_TYPE_MAPS.get(primitiveType);
            if (!UNBOX_TYPE_MAPS.containsKey(wrapperType)) {
                throw new IllegalStateException(
                    "Missing unboxed type mapping for wrapper type: " + wrapperType);
            }
            if (UNBOX_TYPE_MAPS.get(wrapperType) != primitiveType) {
                throw new IllegalStateException(
                    "Inconsistent type mapping between " + primitiveType + " and " + wrapperType);
            }
        }

        // Ensure all unboxed types have corresponding boxed types
        for (Class<?> wrapperType : UNBOX_TYPE_MAPS.keySet()) {
            Class<?> primitiveType = UNBOX_TYPE_MAPS.get(wrapperType);
            if (!BOX_TYPE_MAPS.containsKey(primitiveType)) {
                throw new IllegalStateException(
                    "Missing boxed type mapping for primitive type: " + primitiveType);
            }
        }
    }

    /**
     * Internal class to define primitive type to wrapper type mapping.
     */
    private static class TypeMapping {
        final Class<?> primitiveType;
        final Class<?> wrapperType;

        TypeMapping(Class<?> primitiveType, Class<?> wrapperType) {
            this.primitiveType = primitiveType;
            this.wrapperType = wrapperType;
        }
    }

    public static Method findMatchMethod(Class<?> udfClass, List<Class<?>> paramTypes) {
        return findMatchMethod(udfClass, UDF_EVAL_METHOD_NAME, paramTypes);
    }

    public static Method findMatchMethod(Class<?> clazz, String methodName, List<Class<?>> paramTypes) {
        List<Method> methods = getAllMethod(clazz);
        double maxScore = 0d;
        Method bestMatch = null;
        for (Method method : methods) {
            if (!method.getName().equals(methodName)) {
                continue;
            }
            Class<?>[] defineTypes = method.getParameterTypes();
            double score = getMatchScore(defineTypes, paramTypes.toArray(new Class<?>[]{}));
            if (score > maxScore) {
                maxScore = score;
                bestMatch = method;
            }
        }
        if (bestMatch != null) {
            return bestMatch;
        }
        throw new IllegalArgumentException("Cannot find method " + methodName + " in " + clazz
            + ",input paramType is " + paramTypes);
    }

    private static double getMatchScore(Class<?>[] defineTypes, Class<?>[] callTypes) {

        if (defineTypes.length == 0 && callTypes.length == 0) {
            return 1;
        }

        if (defineTypes.length == 0 && callTypes.length > 0) {
            return 0;
        }

        if (defineTypes.length > callTypes.length) {
            return 0;
        }

        //
        double score = 1.0d;

        int i;
        for (i = 0; i < defineTypes.length - 1; i++) {
            double s = getScore(defineTypes[i], callTypes[i]);
            if (s == 0) {
                return 0;
            }
            score *= s;
            if (score == 0) {
                return 0;
            }
        }
        Class<?> lastDefineType = defineTypes[i];

        // test whether the last is a variable parameter.
        if (lastDefineType.isArray()) {
            Class<?> componentType = lastDefineType.getComponentType();
            if (callTypes[i].isArray()
                && i == callTypes.length - 1) {
                score *= getScore(componentType, callTypes[i].getComponentType());
            } else {
                for (; i < callTypes.length; i++) {
                    double s = getScore(componentType, callTypes[i]);

                    if (s == 0) {
                        return 0;
                    }
                    score *= s;
                }
            }
            return score;
        } else {
            double s = getScore(lastDefineType, callTypes[i]);
            if (s == 0) {
                return 0;
            }
            score *= s;
            if (score > 0 && defineTypes.length == callTypes.length) {
                return score;
            }
        }
        return 0;
    }

    private static double getScore(Class<?> defineType, Class<?> callType) {
        defineType = getBoxType(defineType);
        callType = getBoxType(callType);

        if (defineType == callType) {
            return 1d;
        } else {
            if (callType == null) { // the input parameter is null
                return 1d;
            }
            int typeDegreeIndex = findTypeDegreeIndex(defineType, callType);

            if (typeDegreeIndex != -1) {
                // (0, 0.9]
                return (float) (0.9 * (1 - 0.1 * typeDegreeIndex));
            } else if (defineType.isAssignableFrom(callType)) {
                return 0.5d;
            } else if (callType == BinaryString.class && defineType == String.class) {
                return 0.6d;
            } else {
                return 0;
            }
        }
    }

    private static int findTypeDegreeIndex(Class<?> defineType, Class<?> callType) {

        Class<?>[] degreeTypes = TYPE_DEGREE_MAP.get(callType);
        if (degreeTypes == null) {
            return -1;
        }
        for (int i = 0; i < degreeTypes.length; i++) {
            if (degreeTypes[i] == defineType) {
                return i;
            }
        }
        return -1;
    }

    public static List<Class<?>[]> getAllEvalParamTypes(Class<?> udfClass) {
        List<Method> evalMethods = getAllEvalMethods(udfClass);
        List<Class<?>[]> types = new ArrayList<>();
        for (Method evalMethod : evalMethods) {
            types.add(evalMethod.getParameterTypes());
        }
        return types;
    }

    public static List<Method> getAllEvalMethods(Class<?> udfClass) {
        List<Method> evalMethods = new ArrayList<>();
        Class<?> clazz = udfClass;
        while (clazz != Object.class) {
            Method[] methods = clazz.getDeclaredMethods();
            for (Method method : methods) {
                if (method.getName().equals(UDF_EVAL_METHOD_NAME)) {
                    evalMethods.add(method);
                }
            }
            clazz = clazz.getSuperclass();
        }
        return evalMethods;
    }

    private static List<Method> getAllMethod(Class<?> udfClass) {
        List<Method> evalMethods = new ArrayList<>();
        Class<?> clazz = udfClass;
        while (clazz != Object.class) {
            Method[] methods = clazz.getDeclaredMethods();
            for (Method method : methods) {
                evalMethods.add(method);
            }
            clazz = clazz.getSuperclass();
        }
        return evalMethods;
    }

    /**
     * Gets the boxed (wrapper) type for a primitive type, or returns the type as-is if it's not primitive.
     *
     * @param type the input type
     * @return the boxed type if primitive, otherwise the original type
     */
    public static Class<?> getBoxType(Class<?> type) {
        return BOX_TYPE_MAPS.getOrDefault(type, type);
    }

    /**
     * Gets the unboxed (primitive) type for a wrapper type, or returns the type as-is if it's not a wrapper.
     *
     * @param type the input type
     * @return the unboxed type if wrapper, otherwise the original type
     */
    public static Class<?> getUnboxType(Class<?> type) {
        return UNBOX_TYPE_MAPS.getOrDefault(type, type);
    }

    /**
     * Checks if a type is a primitive wrapper type.
     *
     * @param type the type to check
     * @return true if the type is a wrapper type, false otherwise
     */
    public static boolean isWrapperType(Class<?> type) {
        return UNBOX_TYPE_MAPS.containsKey(type);
    }

    /**
     * Checks if a type is a primitive type.
     *
     * @param type the type to check
     * @return true if the type is primitive, false otherwise
     */
    public static boolean isPrimitiveType(Class<?> type) {
        return BOX_TYPE_MAPS.containsKey(type);
    }


    public static Type[] getUDAFGenericTypes(Class<?> udafClass) {
        return FunctionCallUtils.getGenericTypes(udafClass, UDAF.class);
    }

    public static Type[] getGenericTypes(Class<?> clazz, Class<?> baseClass) {
        if (!baseClass.isAssignableFrom(clazz)) {
            throw new IllegalArgumentException(
                "input clazz must be a sub class of the base class: " + baseClass);
        }

        Map<TypeVariable<? extends Class<?>>, Type> defineTypeMap = new HashMap<>();

        while (clazz != baseClass) {
            Class<?> superClass = clazz.getSuperclass();
            TypeVariable<? extends Class<?>>[] typeVariables = superClass.getTypeParameters();
            Type[] types = ((ParameterizedType) clazz.getGenericSuperclass())
                .getActualTypeArguments();

            for (int i = 0; i < typeVariables.length; i++) {
                TypeVariable<? extends Class<?>> typeVariable = typeVariables[i];
                Type type =
                    types[i] instanceof ParameterizedType ? ((ParameterizedType) types[i]).getRawType() : types[i];
                defineTypeMap.put(typeVariable, type);
            }
            clazz = superClass;
        }

        TypeVariable<? extends Class<?>>[] typeVariables = baseClass.getTypeParameters();
        Type[] types = new Type[typeVariables.length];

        for (int i = 0; i < typeVariables.length; i++) {
            Type type = typeVariables[i];
            do {
                type = defineTypeMap.get(type);
            } while (type != null && !(type instanceof Class));
            types[i] = type;
        }
        return types;
    }


    public static Class<? extends UDAF<?, ?, ?>> findMatchUDAF(String name,
                                                               List<Class<? extends UDAF<?, ?, ?>>> udafClassList,
                                                               List<Class<?>> callTypes) {
        double maxScore = 0;
        Class<? extends UDAF<?, ?, ?>> bestClass = null;
        List<List<Class<?>>> allDefinedTypes = new ArrayList<>();

        for (Class<? extends UDAF<?, ?, ?>> udafClass : udafClassList) {
            List<Class<?>> defineTypes = getUDAFInputTypes(udafClass);
            allDefinedTypes.add(defineTypes);
            if (callTypes.size() != defineTypes.size()) {
                continue;
            }

            double score = 1.0;
            for (int i = 0; i < callTypes.size(); i++) {
                score *= getScore(defineTypes.get(i), callTypes.get(i));
            }
            if (score > maxScore) {
                maxScore = score;
                bestClass = udafClass;
            }
        }
        if (bestClass != null) {
            return bestClass;
        }

        throw new GeaFlowDSLException(
            "Mismatch input types for " + name + ",the input type is " + callTypes
                + ",while the udaf defined type is " + Joiner.on(" OR ").join(allDefinedTypes));
    }

    public static List<Class<?>> getUDAFInputTypes(Class<?> udafClass) {
        List<Class<?>> inputTypes = Lists.newArrayList();
        Type[] genericTypes = getGenericTypes(udafClass, UDAF.class);
        Class<?> inputType = (Class<?>) genericTypes[0];
        // case for UDAF has multi-parameters.
        if (UDAFArguments.class.isAssignableFrom(inputType)) {
            try {
                UDAFArguments input = (UDAFArguments) inputType.newInstance();
                inputTypes.addAll(input.getParamTypes());
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        } else {
            inputTypes.add(inputType);
        }
        return inputTypes;
    }

    public static Object callMethod(Method method, Object target, Object[] params)
        throws InvocationTargetException, IllegalAccessException {

        Class<?>[] defineTypes = method.getParameterTypes();
        int variableParamIndex = -1;

        if (defineTypes.length > 0) {
            Class<?> lastDefineType = defineTypes[defineTypes.length - 1];
            if (lastDefineType.isArray()) {
                if (params[defineTypes.length - 1] != null
                    && params[defineTypes.length - 1].getClass().isArray()
                    && params.length == defineTypes.length) {
                    variableParamIndex = -1;
                } else {
                    variableParamIndex = defineTypes.length - 1;
                }
            }
        }

        int paramSize = variableParamIndex >= 0 ? variableParamIndex + 1 : params.length;
        Object[] castParams = new Object[paramSize];

        if (variableParamIndex >= 0) {
            int i = 0;
            for (; i < variableParamIndex; i++) {
                castParams[i] = TypeCastUtil.cast(params[i], getBoxType(defineTypes[i]));
            }
            Class<?> componentType = defineTypes[variableParamIndex].getComponentType();
            Object[] varParaArray =
                (Object[]) Array.newInstance(componentType, params.length - variableParamIndex);

            for (; i < params.length; i++) {
                varParaArray[i - variableParamIndex] = TypeCastUtil.cast(params[i],
                    getBoxType(componentType));
            }

            castParams[variableParamIndex] = varParaArray;
        } else {
            for (int i = 0; i < params.length; i++) {
                castParams[i] = TypeCastUtil.cast(params[i], getBoxType(defineTypes[i]));
            }
        }
        Object result = method.invoke(target, castParams);
        if (result instanceof String) { // convert string to binary string if the udf return string type.
            result = BinaryString.fromString((String) result);
        }
        return result;
    }

    public static Class<?> typeClass(Class<?> type, boolean useBinary) {
        if (useBinary) {
            if (type == String.class) {
                return BinaryString.class;
            }
            if (type.isArray() && type.getComponentType() == String.class) {
                return Array.newInstance(BinaryString.class, 0).getClass();
            }
        }
        return type;
    }
}
