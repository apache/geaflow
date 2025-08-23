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

package org.apache.geaflow.common.encoder.impl;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.common.encoder.EncoderResolver;
import org.apache.geaflow.common.encoder.Encoders;
import org.apache.geaflow.common.encoder.IEncoder;
import org.apache.geaflow.common.errorcode.RuntimeErrors;
import org.apache.geaflow.common.exception.GeaflowRuntimeException;

public class PojoEncoder<T> extends AbstractEncoder<T> {

    private static final Map<Class<?>, PojoField[]> POJO_FIELDS_CACHE = new HashMap<>();

    private final Class<T> clazz;
    private PojoField[] pojoFields;

    public static PojoField[] getPojoFields(Class<?> clazz) {
        if (!POJO_FIELDS_CACHE.containsKey(clazz)) {
            synchronized (PojoEncoder.class) {
                if (!POJO_FIELDS_CACHE.containsKey(clazz)) {
                    List<Field> fields = Arrays.stream(clazz.getDeclaredFields())
                        .filter(field -> !Modifier.isTransient(field.getModifiers()) && !Modifier.isStatic(field.getModifiers()))
                        .sorted(Comparator.comparing(Field::getName))
                        .peek(f -> f.setAccessible(true))
                        .collect(Collectors.toList());

                    List<PojoField> pojoFields = new ArrayList<>();
                    for (Field field : fields) {
                        Class<?> fieldType = field.getType();
                        IEncoder<?> encoder = EncoderResolver.resolveClass(fieldType);
                        pojoFields.add(PojoField.build(field, encoder));
                    }

                    PojoField[] fieldsArr = pojoFields.stream()
                        .peek(f -> f.getField().setAccessible(true))
                        .sorted(Comparator.comparing(f -> f.getField().getName()))
                        .toArray(PojoField[]::new);
                    POJO_FIELDS_CACHE.put(clazz, fieldsArr);
                }
            }
        }
        return POJO_FIELDS_CACHE.get(clazz);
    }

    public static <T> PojoEncoder<T> build(Class<T> clazz) {
        return new PojoEncoder<>(clazz);
    }

    public PojoEncoder(Class<T> clazz) {
        this.clazz = clazz;
    }

    @Override
    public void init(Configuration config) {
        if (this.pojoFields == null) {
            this.pojoFields = getPojoFields(this.clazz);
        }
    }

    @Override
    public void encode(T data, OutputStream outputStream) throws IOException {
        if (data == null) {
            Encoders.INTEGER.encode(NULL, outputStream);
            return;
        } else {
            Encoders.INTEGER.encode(NOT_NULL, outputStream);
        }
        try {
            for (int i = 0; i < this.pojoFields.length; i++) {
                PojoField pojoField = this.pojoFields[i];
                Object value = pojoField.getField().get(data);
                if (value == null) {
                    Encoders.INTEGER.encode(NULL, outputStream);
                } else {
                    Encoders.INTEGER.encode(NOT_NULL, outputStream);
                    ((IEncoder<Object>) pojoField.getEncoder()).encode(value, outputStream);
                }
            }
        } catch (IllegalAccessException e) {
            throw new GeaflowRuntimeException(RuntimeErrors.INST.typeSysError(e.getMessage()), e);
        }

    }

    @Override
    public T decode(InputStream inputStream) throws IOException {
        if (Encoders.INTEGER.decode(inputStream) == NULL) {
            return null;
        }
        try {
            T obj = clazz.newInstance();
            for (int i = 0; i < this.pojoFields.length; i++) {
                int flag = Encoders.INTEGER.decode(inputStream);
                if (flag == NOT_NULL) {
                    PojoField pojoField = this.pojoFields[i];
                    Object value = pojoField.getEncoder().decode(inputStream);
                    pojoField.getField().set(obj, value);
                }
            }
            return obj;
        } catch (InstantiationException | IllegalAccessException e) {
            throw new GeaflowRuntimeException(RuntimeErrors.INST.typeSysError(e.getMessage()), e);
        }

    }

    public static class PojoField implements Serializable {

        private final Field field;
        private final IEncoder<?> encoder;

        public static PojoField build(Field field, IEncoder<?> encoder) {
            return new PojoField(field, encoder);
        }

        public PojoField(Field field, IEncoder<?> encoder) {
            this.field = field;
            this.encoder = encoder;
        }

        public Field getField() {
            return this.field;
        }

        public IEncoder<?> getEncoder() {
            return this.encoder;
        }

    }

}
