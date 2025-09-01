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

    private static final Map<Class<?>, Class<?>[]> TYPE_DEGREE_MAP = new HashMap<>();

    static {
        TYPE_DEGREE_MAP
            .put(Integer.class, new Class<?>[]{Long.class, Double.class, BigDecimal.class});
        TYPE_DEGREE_MAP.put(Long.class, new Class<?>[]{Double.class, BigDecimal.class});
        TYPE_DEGREE_MAP.put(Byte.class,
            new Class<?>[]{Integer.class, Long.class, Double.class, BigDecimal.class});
        TYPE_DEGREE_MAP.put(Short.class,
            new Class<?>[]{Integer.class, Long.class, Double.class, BigDecimal.class});
        TYPE_DEGREE_MAP.put(BigDecimal.class, new Class<?>[]{Double.class});
    }

    private static final Map<Class<?>, Class<?>> BOX_TYPE_MAPS = new HashMap<>();
    private static final Map<Class<?>, Class<?>> UNBOX_TYPE_MAPS = new HashMap<>();

    static {
        BOX_TYPE_MAPS.put(int.class, Integer.class);
        BOX_TYPE_MAPS.put(long.class, Long.class);
        BOX_TYPE_MAPS.put(short.class, Short.class);
        BOX_TYPE_MAPS.put(byte.class, Byte.class);
        BOX_TYPE_MAPS.put(boolean.class, Boolean.class);
        BOX_TYPE_MAPS.put(double.class, Double.class);

        UNBOX_TYPE_MAPS.put(Integer.class, int.class);
        UNBOX_TYPE_MAPS.put(Long.class, long.class);
        UNBOX_TYPE_MAPS.put(Short.class, short.class);
        UNBOX_TYPE_MAPS.put(Byte.class, byte.class);
        UNBOX_TYPE_MAPS.put(Boolean.class, boolean.class);
        UNBOX_TYPE_MAPS.put(Double.class, double.class);
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

    public static List<Class[]> getAllEvalParamTypes(Class<?> udfClass) {
        List<Method> evalMethods = getAllEvalMethods(udfClass);
        List<Class[]> types = new ArrayList<>();
        for (Method evalMethod : evalMethods) {
            types.add(evalMethod.getParameterTypes());
        }
        return types;
    }

    public static List<Method> getAllEvalMethods(Class<?> udfClass) {
        List<Method> evalMethods = new ArrayList<>();
        Class clazz = udfClass;
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
        Class clazz = udfClass;
        while (clazz != Object.class) {
            Method[] methods = clazz.getDeclaredMethods();
            for (Method method : methods) {
                evalMethods.add(method);
            }
            clazz = clazz.getSuperclass();
        }
        return evalMethods;
    }

    public static Class<?> getBoxType(Class<?> type) {
        return BOX_TYPE_MAPS.getOrDefault(type, type);
    }

    public static Class<?> getUnboxType(Class<?> type) {
        return UNBOX_TYPE_MAPS.getOrDefault(type, type);
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
