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

package org.apache.geaflow.common.encoder;

import com.google.common.annotations.VisibleForTesting;
import java.lang.reflect.Array;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.geaflow.common.encoder.impl.EnumEncoder;
import org.apache.geaflow.common.encoder.impl.GenericArrayEncoder;
import org.apache.geaflow.common.encoder.impl.PojoEncoder;
import org.apache.geaflow.common.errorcode.RuntimeErrors;
import org.apache.geaflow.common.exception.GeaflowRuntimeException;
import org.apache.geaflow.common.tuple.Triple;
import org.apache.geaflow.common.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EncoderResolver {

    private static final Logger LOGGER = LoggerFactory.getLogger(EncoderResolver.class);

    private static final String EMPTY = "";
    private static final String UNDERLINE = "_";
    private static final String METHOD_READ_OBJECT = "readObject";
    private static final String METHOD_WRITE_OBJECT = "writeObject";

    public static IEncoder<?> resolveClass(Class<?> clazz) {
        return resolveType(clazz);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    public static IEncoder<?> resolveType(Type type) {
        if (isClassType(type)) {
            Class<?> clazz = typeToClass(type);
            if (clazz == Object.class) {
                return null;
            }
            if (clazz == Class.class) {
                return null;
            }
            if (Modifier.isInterface(clazz.getModifiers())) {
                return null;
            }
            if (Encoders.PRIMITIVE_ENCODER_MAP.containsKey(clazz)) {
                return Encoders.PRIMITIVE_ENCODER_MAP.get(clazz);
            }
            if (Tuple.class.isAssignableFrom(clazz)) {
                return resolveTuple(type);
            }
            if (Triple.class.isAssignableFrom(clazz)) {
                return resolveTriple(type);
            }
            if (Enum.class.isAssignableFrom(clazz)) {
                return new EnumEncoder<>(clazz);
            }
            if (clazz.isArray()) {
                if (Encoders.PRIMITIVE_ARR_ENCODER_MAP.containsKey(clazz)) {
                    return Encoders.PRIMITIVE_ARR_ENCODER_MAP.get(clazz);
                }
                Class<?> componentClass = clazz.getComponentType();
                IEncoder<?> componentEncoder = resolveClass(clazz.getComponentType());
                if (componentEncoder != null) {
                    GenericArrayEncoder.ArrayConstructor<?> constructor =
                        length -> (Object[]) Array.newInstance(componentClass, length);
                    return new GenericArrayEncoder(componentEncoder, constructor);
                }
            }
            return resolvePojo(type);
        }
        return null;
    }

    public static IEncoder<?> resolveTuple(Type type) {
        List<ParameterizedType> subTypeTree = new ArrayList<>();
        Type curType = type;
        while (!(isClassType(curType) && typeToClass(curType).equals(Tuple.class))) {
            if (curType instanceof ParameterizedType) {
                subTypeTree.add((ParameterizedType) curType);
            }
            curType = typeToClass(curType).getGenericSuperclass();
        }

        if (curType instanceof Class) {
            LOGGER.warn("Tuple needs to be parameterized with generics");
            return null;
        }

        ParameterizedType parameterizedType = (ParameterizedType) curType;
        subTypeTree.add(parameterizedType);

        IEncoder<?>[] subEncoders = resolveSubEncoder(subTypeTree, parameterizedType);
        if (subEncoders == null || subEncoders.length != 2) {
            return null;
        }
        if (countClassFields(typeToClass(type)) != subEncoders.length) {
            LOGGER.warn("tuple filed num does not match encoder num");
            return null;
        }
        return Encoders.tuple(subEncoders[0], subEncoders[1]);
    }

    public static IEncoder<?> resolveTriple(Type type) {
        List<ParameterizedType> subTypeTree = new ArrayList<>();
        Type curType = type;
        while (!(isClassType(curType) && typeToClass(curType).equals(Triple.class))) {
            if (curType instanceof ParameterizedType) {
                subTypeTree.add((ParameterizedType) curType);
            }
            curType = typeToClass(curType).getGenericSuperclass();
        }

        if (curType instanceof Class) {
            LOGGER.warn("Tuple needs to be parameterized with generics");
            return null;
        }

        ParameterizedType parameterizedType = (ParameterizedType) curType;
        subTypeTree.add(parameterizedType);

        IEncoder<?>[] subEncoders = resolveSubEncoder(subTypeTree, parameterizedType);
        if (subEncoders == null || subEncoders.length != 3) {
            return null;
        }
        if (countClassFields(typeToClass(type)) != subEncoders.length) {
            LOGGER.warn("triple filed num does not match encoder num");
            return null;
        }
        return Encoders.triple(subEncoders[0], subEncoders[1], subEncoders[2]);
    }

    private static IEncoder<?>[] resolveSubEncoder(List<ParameterizedType> typeTree,
                                                   ParameterizedType parameterizedType) {
        Type[] typeArguments = parameterizedType.getActualTypeArguments();
        IEncoder<?>[] encoders = new IEncoder<?>[typeArguments.length];
        for (int i = 0; i < typeArguments.length; i++) {
            Type typeArgument = typeArguments[i];
            Type concreteType = typeArgument;
            if (typeArgument instanceof TypeVariable) {
                concreteType = getConcreteTypeofTypeVariable(typeTree, (TypeVariable<?>) typeArgument);
            }
            IEncoder<?> encoder = resolveType(concreteType);
            if (encoder == null) {
                return null;
            }
            encoders[i] = encoder;
        }
        return encoders;
    }

    public static IEncoder<?> resolvePojo(Type type) {
        if (isClassType(type)) {
            Class<?> clazz = typeToClass(type);
            try {
                analysisPojo(clazz);
            } catch (GeaflowRuntimeException e) {
                return null;
            }
            return PojoEncoder.build(clazz);
        }
        return null;
    }

    @VisibleForTesting
    protected static <T> void analysisPojo(Class<T> clazz) {
        if (!Modifier.isPublic(clazz.getModifiers())) {
            String msg = "Class [" + clazz.getName() + "] is not public, it cannot be used as a POJO type";
            throw new GeaflowRuntimeException(RuntimeErrors.INST.typeSysError(msg));
        }
        if (clazz.isInterface() || Modifier.isAbstract(clazz.getModifiers())) {
            String msg = "Class [" + clazz.getName() + "] is abstract or an interface,"
                + "it cannot be used as a POJO type";
            throw new GeaflowRuntimeException(RuntimeErrors.INST.typeSysError(msg));
        }
        if (clazz.getSuperclass() != Object.class) {
            String msg = "Class [" + clazz.getName() + "] does not extends Object directly, "
                + "it cannot be used as a POJO type";
            throw new GeaflowRuntimeException(RuntimeErrors.INST.typeSysError(msg));
        }
        Method[] methods = clazz.getDeclaredMethods();
        for (Method method : methods) {
            if (METHOD_READ_OBJECT.equals(method.getName()) || METHOD_WRITE_OBJECT.equals(method.getName())) {
                String msg = "Class [" + clazz.getName() + "] contains custom serialization methods we do not call, "
                    + "it cannot be used as a POJO type";
                throw new GeaflowRuntimeException(RuntimeErrors.INST.typeSysError(msg));
            }
        }

        // check default constructor
        Constructor<T> defaultConstructor;
        try {
            defaultConstructor = clazz.getDeclaredConstructor();
        } catch (NoSuchMethodException e) {
            String msg = "Class [" + clazz.getName() + "] has no default constructor, it cannot be used as a POJO type";
            throw new GeaflowRuntimeException(RuntimeErrors.INST.typeSysError(msg), e);
        }
        if (!Modifier.isPublic(defaultConstructor.getModifiers())) {
            String msg = "The default constructor of [" + clazz + "] is not Public, it cannot be used as a POJO type";
            throw new GeaflowRuntimeException(RuntimeErrors.INST.typeSysError(msg));
        }

        List<Field> fields = getPojoFields(clazz);
        if (fields.isEmpty()) {
            String msg = "Class [" + clazz.getName() + "] has no declared fields";
            throw new GeaflowRuntimeException(RuntimeErrors.INST.typeSysError(msg));
        }
        for (Field field : fields) {
            checkValidPojoField(field, clazz);
        }
    }

    private static void checkValidPojoField(Field f, Class<?> clazz) {
        if (!Modifier.isPublic(f.getModifiers())) {
            final String fieldNameLow = f.getName().toLowerCase().replaceAll(UNDERLINE, EMPTY);

            Type fieldType = f.getGenericType();
            Class<?> fieldTypeWrapper = f.getType();
            if (fieldTypeWrapper.isPrimitive()) {
                fieldTypeWrapper = Encoders.PRIMITIVE_WRAPPER_MAP.get(fieldTypeWrapper);
            }
            if (fieldType instanceof TypeVariable) {
                String msg = "do not support generics yet";
                throw new GeaflowRuntimeException(RuntimeErrors.INST.typeSysError(msg));
            }

            boolean hasGetter = false;
            boolean hasSetter = false;
            for (Method m : clazz.getMethods()) {
                final String methodNameLow = m.getName().toLowerCase().replaceAll(UNDERLINE, EMPTY);
                // check getter, one qualified method is ok
                if (!hasGetter
                    && (methodNameLow.equals("get" + fieldNameLow) || methodNameLow.equals("is" + fieldNameLow))
                    && m.getParameterTypes().length == 0
                    && (m.getGenericReturnType().equals(fieldType) || m.getReturnType().equals(fieldTypeWrapper))
                ) {
                    hasGetter = true;
                }
                // check setter, one qualified method is ok
                if (!hasSetter
                    && methodNameLow.equals("set" + fieldNameLow)
                    && m.getParameterTypes().length == 1
                    && (m.getGenericParameterTypes()[0].equals(fieldType) || m.getParameterTypes()[0].equals(fieldTypeWrapper))
                    && (m.getReturnType().equals(Void.TYPE) || m.getReturnType().equals(clazz))
                ) {
                    hasSetter = true;
                }
            }

            if (!hasGetter) {
                String msg = clazz + " does not contain a getter for field " + f.getName();
                throw new GeaflowRuntimeException(RuntimeErrors.INST.typeSysError(msg));
            }
            if (!hasSetter) {
                String msg = clazz + " does not contain a setter for field " + f.getName();
                throw new GeaflowRuntimeException(RuntimeErrors.INST.typeSysError(msg));
            }
        }
    }

    /**
     * Resolve encoder from function generics.
     */

    public static IEncoder<?> resolveFunction(Class<?> baseClass, Object function) {
        return resolveFunction(baseClass, function, 0);
    }

    public static IEncoder<?> resolveFunction(Class<?> baseClass, Object function, int typeParaIdx) {
        if (baseClass == Object.class) {
            return null;
        }
        Class<?> funcClass = function.getClass();
        List<ParameterizedType> typeTree = new ArrayList<>();
        Type paraType = extractParaType(typeTree, baseClass, funcClass, typeParaIdx);

        if (paraType instanceof TypeVariable) {
            Type concreteType = getConcreteTypeofTypeVariable(typeTree, (TypeVariable<?>) paraType);
            return resolveType(concreteType);
        }

        return resolveType(paraType);
    }

    private static Type extractParaType(List<ParameterizedType> typeTree,
                                        Class<?> baseClass,
                                        Class<?> functionClass,
                                        int typeParaIdx) {
        Type[] gInterfaces = functionClass.getGenericInterfaces();
        for (Type gInterface : gInterfaces) {
            Type type = extractParaTypeFromGeneric(typeTree, baseClass, gInterface, typeParaIdx);
            if (type != null) {
                return type;
            }
        }
        Type gClass = functionClass.getGenericSuperclass();
        return extractParaTypeFromGeneric(typeTree, baseClass, gClass, typeParaIdx);
    }

    private static Type extractParaTypeFromGeneric(List<ParameterizedType> typeTree,
                                                   Class<?> baseClass,
                                                   Type type,
                                                   int typeParaIdx) {
        if (type instanceof ParameterizedType) {
            ParameterizedType parameterizedType = (ParameterizedType) type;
            typeTree.add(parameterizedType);
            Class<?> rawType = (Class<?>) parameterizedType.getRawType();
            if (baseClass.equals(rawType)) {
                return parameterizedType.getActualTypeArguments()[typeParaIdx];
            }
            if (baseClass.isAssignableFrom(rawType)) {
                return extractParaType(typeTree, baseClass, rawType, typeParaIdx);
            }
        }
        if (type instanceof Class) {
            Class<?> clazz = (Class<?>) type;
            if (baseClass.isAssignableFrom(clazz)) {
                return extractParaType(typeTree, baseClass, clazz, typeParaIdx);
            }
        }
        return null;
    }

    private static Type getConcreteTypeofTypeVariable(List<ParameterizedType> typeTree, TypeVariable<?> typeVar) {
        TypeVariable<?> curTypeVar = typeVar;
        for (int i = typeTree.size() - 1; i >= 0; i--) {
            ParameterizedType curType = typeTree.get(i);
            Class<?> rawType = (Class<?>) curType.getRawType();
            TypeVariable<? extends Class<?>>[] rawTypeParameters = rawType.getTypeParameters();
            for (int idx = 0; idx < rawTypeParameters.length; idx++) {
                TypeVariable<?> rawTypeVar = rawType.getTypeParameters()[idx];
                // check if variable match
                if (curTypeVar.getName().equals(rawTypeVar.getName())
                    && curTypeVar.getGenericDeclaration().equals(rawTypeVar.getGenericDeclaration())) {
                    Type actualTypeArgument = curType.getActualTypeArguments()[idx];
                    if (actualTypeArgument instanceof TypeVariable<?>) {
                        // another type variable level
                        curTypeVar = (TypeVariable<?>) actualTypeArgument;
                    } else {
                        // class
                        return actualTypeArgument;
                    }
                }
            }
        }
        // most likely type erasure
        return curTypeVar;
    }

    public static boolean isClassType(Type t) {
        return t instanceof Class<?> || t instanceof ParameterizedType;
    }

    public static Class<?> typeToClass(Type t) {
        if (t instanceof Class) {
            return (Class<?>) t;
        } else if (t instanceof ParameterizedType) {
            return ((Class<?>) ((ParameterizedType) t).getRawType());
        }
        throw new GeaflowRuntimeException(RuntimeErrors.INST.typeSysError("Cannot convert type to class"));
    }

    private static int countClassFields(Class<?> clazz) {
        int fieldCount = 0;
        for (Field field : clazz.getFields()) {
            if (!Modifier.isStatic(field.getModifiers())
                && !Modifier.isTransient(field.getModifiers())) {
                fieldCount++;
            }
        }
        return fieldCount;
    }

    private static List<Field> getPojoFields(Class<?> clazz) {
        return Arrays.stream(clazz.getDeclaredFields())
            .filter(field -> !Modifier.isTransient(field.getModifiers()) && !Modifier.isStatic(field.getModifiers()))
            .collect(Collectors.toList());
    }

}
