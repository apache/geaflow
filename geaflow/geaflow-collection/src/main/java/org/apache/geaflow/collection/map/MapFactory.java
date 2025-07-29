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

package org.apache.geaflow.collection.map;

import it.unimi.dsi.fastutil.bytes.Byte2BooleanOpenHashMap;
import it.unimi.dsi.fastutil.bytes.Byte2ByteArrayMap;
import it.unimi.dsi.fastutil.bytes.Byte2ByteOpenHashMap;
import it.unimi.dsi.fastutil.bytes.Byte2DoubleOpenHashMap;
import it.unimi.dsi.fastutil.bytes.Byte2FloatOpenHashMap;
import it.unimi.dsi.fastutil.bytes.Byte2IntOpenHashMap;
import it.unimi.dsi.fastutil.bytes.Byte2LongOpenHashMap;
import it.unimi.dsi.fastutil.bytes.Byte2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.bytes.Byte2ShortOpenHashMap;
import it.unimi.dsi.fastutil.doubles.Double2BooleanOpenHashMap;
import it.unimi.dsi.fastutil.doubles.Double2ByteArrayMap;
import it.unimi.dsi.fastutil.doubles.Double2ByteOpenHashMap;
import it.unimi.dsi.fastutil.doubles.Double2DoubleOpenHashMap;
import it.unimi.dsi.fastutil.doubles.Double2FloatOpenHashMap;
import it.unimi.dsi.fastutil.doubles.Double2IntOpenHashMap;
import it.unimi.dsi.fastutil.doubles.Double2LongOpenHashMap;
import it.unimi.dsi.fastutil.doubles.Double2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.doubles.Double2ShortOpenHashMap;
import it.unimi.dsi.fastutil.floats.Float2BooleanOpenHashMap;
import it.unimi.dsi.fastutil.floats.Float2ByteArrayMap;
import it.unimi.dsi.fastutil.floats.Float2ByteOpenHashMap;
import it.unimi.dsi.fastutil.floats.Float2DoubleOpenHashMap;
import it.unimi.dsi.fastutil.floats.Float2FloatOpenHashMap;
import it.unimi.dsi.fastutil.floats.Float2IntOpenHashMap;
import it.unimi.dsi.fastutil.floats.Float2LongOpenHashMap;
import it.unimi.dsi.fastutil.floats.Float2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.floats.Float2ShortOpenHashMap;
import it.unimi.dsi.fastutil.ints.Int2BooleanOpenHashMap;
import it.unimi.dsi.fastutil.ints.Int2ByteArrayMap;
import it.unimi.dsi.fastutil.ints.Int2ByteOpenHashMap;
import it.unimi.dsi.fastutil.ints.Int2DoubleOpenHashMap;
import it.unimi.dsi.fastutil.ints.Int2FloatOpenHashMap;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import it.unimi.dsi.fastutil.ints.Int2LongOpenHashMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.ints.Int2ShortOpenHashMap;
import it.unimi.dsi.fastutil.longs.Long2BooleanOpenHashMap;
import it.unimi.dsi.fastutil.longs.Long2ByteArrayMap;
import it.unimi.dsi.fastutil.longs.Long2ByteOpenHashMap;
import it.unimi.dsi.fastutil.longs.Long2DoubleOpenHashMap;
import it.unimi.dsi.fastutil.longs.Long2FloatOpenHashMap;
import it.unimi.dsi.fastutil.longs.Long2IntOpenHashMap;
import it.unimi.dsi.fastutil.longs.Long2LongOpenHashMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.longs.Long2ShortOpenHashMap;
import it.unimi.dsi.fastutil.objects.Object2BooleanOpenHashMap;
import it.unimi.dsi.fastutil.objects.Object2ByteArrayMap;
import it.unimi.dsi.fastutil.objects.Object2ByteOpenHashMap;
import it.unimi.dsi.fastutil.objects.Object2DoubleOpenHashMap;
import it.unimi.dsi.fastutil.objects.Object2FloatOpenHashMap;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import it.unimi.dsi.fastutil.objects.Object2LongOpenHashMap;
import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.objects.Object2ShortOpenHashMap;
import it.unimi.dsi.fastutil.shorts.Short2BooleanOpenHashMap;
import it.unimi.dsi.fastutil.shorts.Short2ByteArrayMap;
import it.unimi.dsi.fastutil.shorts.Short2ByteOpenHashMap;
import it.unimi.dsi.fastutil.shorts.Short2DoubleOpenHashMap;
import it.unimi.dsi.fastutil.shorts.Short2FloatOpenHashMap;
import it.unimi.dsi.fastutil.shorts.Short2IntOpenHashMap;
import it.unimi.dsi.fastutil.shorts.Short2LongOpenHashMap;
import it.unimi.dsi.fastutil.shorts.Short2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.shorts.Short2ShortOpenHashMap;
import java.util.HashMap;
import java.util.Map;
import org.apache.geaflow.collection.PrimitiveType;

public class MapFactory {

    private static final Map<Class, MapFactoryAdaptor> ADAPTOR_MAP = new HashMap<>();
    private static final MapFactoryAdaptor DEFAULT_ADAPTOR = new ObjectMapFactoryAdaptor();

    static {
        ADAPTOR_MAP.put(Integer.class, new IntMapFactoryAdaptor());
        ADAPTOR_MAP.put(Integer.TYPE, new IntMapFactoryAdaptor());
        ADAPTOR_MAP.put(Byte.class, new ByteMapFactoryAdaptor());
        ADAPTOR_MAP.put(Byte.TYPE, new ByteMapFactoryAdaptor());
        ADAPTOR_MAP.put(Double.class, new DoubleMapFactoryAdaptor());
        ADAPTOR_MAP.put(Double.TYPE, new DoubleMapFactoryAdaptor());
        ADAPTOR_MAP.put(Long.class, new LongMapFactoryAdaptor());
        ADAPTOR_MAP.put(Long.TYPE, new LongMapFactoryAdaptor());
        ADAPTOR_MAP.put(Float.class, new FloatMapFactoryAdaptor());
        ADAPTOR_MAP.put(Float.TYPE, new FloatMapFactoryAdaptor());
        ADAPTOR_MAP.put(Short.class, new ShortMapFactoryAdaptor());
        ADAPTOR_MAP.put(Short.TYPE, new ShortMapFactoryAdaptor());
    }

    public static <K, V> Map buildMap(Class<K> key, Class<V> value) {
        MapFactoryAdaptor adaptor = ADAPTOR_MAP.get(key);
        if (adaptor != null) {
            return adaptor.buildMap(value);
        }

        return DEFAULT_ADAPTOR.buildMap(value);
    }

    public interface MapFactoryAdaptor {

        <K, V> Map<K, V> buildMap(Class<V> value);
    }

    public static class ByteMapFactoryAdaptor implements MapFactoryAdaptor {

        @Override
        public <K, V> Map<K, V> buildMap(Class<V> value) {
            switch (PrimitiveType.getEnum(value.getSimpleName())) {
                case INT:
                    return (Map<K, V>) new Byte2IntOpenHashMap();
                case LONG:
                    return (Map<K, V>) new Byte2LongOpenHashMap();
                case BYTE:
                    return (Map<K, V>) new Byte2ByteOpenHashMap();
                case FLOAT:
                    return (Map<K, V>) new Byte2FloatOpenHashMap();
                case BOOLEAN:
                    return (Map<K, V>) new Byte2BooleanOpenHashMap();
                case SHORT:
                    return (Map<K, V>) new Byte2ShortOpenHashMap();
                case DOUBLE:
                    return (Map<K, V>) new Byte2DoubleOpenHashMap();
                case BYTE_ARRAY:
                    return (Map<K, V>) new Byte2ByteArrayMap();
                default:
                    return (Map<K, V>) new Byte2ObjectOpenHashMap<V>();
            }
        }
    }

    public static class DoubleMapFactoryAdaptor implements MapFactoryAdaptor {

        @Override
        public <K, V> Map<K, V> buildMap(Class<V> value) {
            switch (PrimitiveType.getEnum(value.getSimpleName())) {
                case INT:
                    return (Map<K, V>) new Double2IntOpenHashMap();
                case LONG:
                    return (Map<K, V>) new Double2LongOpenHashMap();
                case BYTE:
                    return (Map<K, V>) new Double2ByteOpenHashMap();
                case FLOAT:
                    return (Map<K, V>) new Double2FloatOpenHashMap();
                case BOOLEAN:
                    return (Map<K, V>) new Double2BooleanOpenHashMap();
                case SHORT:
                    return (Map<K, V>) new Double2ShortOpenHashMap();
                case DOUBLE:
                    return (Map<K, V>) new Double2DoubleOpenHashMap();
                case BYTE_ARRAY:
                    return (Map<K, V>) new Double2ByteArrayMap();
                default:
                    return (Map<K, V>) new Double2ObjectOpenHashMap<V>();
            }
        }
    }

    public static class FloatMapFactoryAdaptor implements MapFactoryAdaptor {

        @Override
        public <K, V> Map<K, V> buildMap(Class<V> value) {
            switch (PrimitiveType.getEnum(value.getSimpleName())) {
                case INT:
                    return (Map<K, V>) new Float2IntOpenHashMap();
                case LONG:
                    return (Map<K, V>) new Float2LongOpenHashMap();
                case BYTE:
                    return (Map<K, V>) new Float2ByteOpenHashMap();
                case FLOAT:
                    return (Map<K, V>) new Float2FloatOpenHashMap();
                case BOOLEAN:
                    return (Map<K, V>) new Float2BooleanOpenHashMap();
                case SHORT:
                    return (Map<K, V>) new Float2ShortOpenHashMap();
                case DOUBLE:
                    return (Map<K, V>) new Float2DoubleOpenHashMap();
                case BYTE_ARRAY:
                    return (Map<K, V>) new Float2ByteArrayMap();
                default:
                    return (Map<K, V>) new Float2ObjectOpenHashMap<V>();
            }
        }
    }

    public static class IntMapFactoryAdaptor implements MapFactoryAdaptor {

        @Override
        public <K, V> Map<K, V> buildMap(Class<V> value) {
            switch (PrimitiveType.getEnum(value.getSimpleName())) {
                case INT:
                    return (Map<K, V>) new Int2IntOpenHashMap();
                case LONG:
                    return (Map<K, V>) new Int2LongOpenHashMap();
                case BYTE:
                    return (Map<K, V>) new Int2ByteOpenHashMap();
                case FLOAT:
                    return (Map<K, V>) new Int2FloatOpenHashMap();
                case BOOLEAN:
                    return (Map<K, V>) new Int2BooleanOpenHashMap();
                case SHORT:
                    return (Map<K, V>) new Int2ShortOpenHashMap();
                case DOUBLE:
                    return (Map<K, V>) new Int2DoubleOpenHashMap();
                case BYTE_ARRAY:
                    return (Map<K, V>) new Int2ByteArrayMap();
                default:
                    return (Map<K, V>) new Int2ObjectOpenHashMap();
            }
        }
    }

    public static class LongMapFactoryAdaptor implements MapFactoryAdaptor {

        @Override
        public <K, V> Map<K, V> buildMap(Class<V> value) {
            switch (PrimitiveType.getEnum(value.getSimpleName())) {
                case INT:
                    return (Map<K, V>) new Long2IntOpenHashMap();
                case LONG:
                    return (Map<K, V>) new Long2LongOpenHashMap();
                case BYTE:
                    return (Map<K, V>) new Long2ByteOpenHashMap();
                case FLOAT:
                    return (Map<K, V>) new Long2FloatOpenHashMap();
                case BOOLEAN:
                    return (Map<K, V>) new Long2BooleanOpenHashMap();
                case SHORT:
                    return (Map<K, V>) new Long2ShortOpenHashMap();
                case DOUBLE:
                    return (Map<K, V>) new Long2DoubleOpenHashMap();
                case BYTE_ARRAY:
                    return (Map<K, V>) new Long2ByteArrayMap();
                default:
                    return (Map<K, V>) new Long2ObjectOpenHashMap();
            }
        }
    }

    public static class ShortMapFactoryAdaptor implements MapFactoryAdaptor {

        public <K, V> Map<K, V> buildMap(Class<V> value) {
            switch (PrimitiveType.getEnum(value.getSimpleName())) {
                case INT:
                    return (Map<K, V>) new Short2IntOpenHashMap();
                case LONG:
                    return (Map<K, V>) new Short2LongOpenHashMap();
                case BYTE:
                    return (Map<K, V>) new Short2ByteOpenHashMap();
                case FLOAT:
                    return (Map<K, V>) new Short2FloatOpenHashMap();
                case BOOLEAN:
                    return (Map<K, V>) new Short2BooleanOpenHashMap();
                case SHORT:
                    return (Map<K, V>) new Short2ShortOpenHashMap();
                case DOUBLE:
                    return (Map<K, V>) new Short2DoubleOpenHashMap();
                case BYTE_ARRAY:
                    return (Map<K, V>) new Short2ByteArrayMap();
                default:
                    return (Map<K, V>) new Short2ObjectOpenHashMap();
            }
        }
    }

    public static class ObjectMapFactoryAdaptor implements MapFactoryAdaptor {

        @Override
        public <K, V> Map<K, V> buildMap(Class<V> value) {
            switch (PrimitiveType.getEnum(value.getSimpleName())) {
                case INT:
                    return (Map<K, V>) new Object2IntOpenHashMap();
                case LONG:
                    return (Map<K, V>) new Object2LongOpenHashMap();
                case BYTE:
                    return (Map<K, V>) new Object2ByteOpenHashMap();
                case FLOAT:
                    return (Map<K, V>) new Object2FloatOpenHashMap();
                case BOOLEAN:
                    return (Map<K, V>) new Object2BooleanOpenHashMap();
                case SHORT:
                    return (Map<K, V>) new Object2ShortOpenHashMap();
                case DOUBLE:
                    return (Map<K, V>) new Object2DoubleOpenHashMap();
                case BYTE_ARRAY:
                    return (Map<K, V>) new Object2ByteArrayMap();
                default:
                    return (Map<K, V>) new Object2ObjectOpenHashMap();
            }
        }
    }
}
