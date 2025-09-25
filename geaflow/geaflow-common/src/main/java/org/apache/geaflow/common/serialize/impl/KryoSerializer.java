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

package org.apache.geaflow.common.serialize.impl;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.serializers.ClosureSerializer;
import de.javakaffee.kryoserializers.ArraysAsListSerializer;
import de.javakaffee.kryoserializers.CollectionsEmptyListSerializer;
import de.javakaffee.kryoserializers.CollectionsSingletonListSerializer;
import de.javakaffee.kryoserializers.UnmodifiableCollectionsSerializer;
import de.javakaffee.kryoserializers.guava.ArrayListMultimapSerializer;
import de.javakaffee.kryoserializers.guava.HashMultimapSerializer;
import de.javakaffee.kryoserializers.guava.ImmutableListSerializer;
import de.javakaffee.kryoserializers.guava.ImmutableMapSerializer;
import de.javakaffee.kryoserializers.guava.ImmutableMultimapSerializer;
import de.javakaffee.kryoserializers.guava.ImmutableSetSerializer;
import de.javakaffee.kryoserializers.guava.ImmutableSortedSetSerializer;
import de.javakaffee.kryoserializers.guava.LinkedHashMultimapSerializer;
import de.javakaffee.kryoserializers.guava.LinkedListMultimapSerializer;
import de.javakaffee.kryoserializers.guava.ReverseListSerializer;
import de.javakaffee.kryoserializers.guava.TreeMultimapSerializer;
import de.javakaffee.kryoserializers.guava.UnmodifiableNavigableSetSerializer;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.geaflow.common.exception.GeaflowRuntimeException;
import org.apache.geaflow.common.serialize.ISerializer;
import org.apache.geaflow.common.serialize.kryo.SubListSerializers4Jdk9;
import org.apache.geaflow.common.utils.ClassUtil;
import org.objenesis.strategy.StdInstantiatorStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KryoSerializer implements ISerializer {

    private static final Logger LOGGER = LoggerFactory.getLogger(KryoSerializer.class);
    private static final int INITIAL_BUFFER_SIZE = 64 * 1024; // 64KB buffer to prevent Buffer underflow
    private static List<String> needRegisterClasses;
    private static Map<Class, Serializer> registeredSerializers;

    private final ThreadLocal<Kryo> local = new ThreadLocal<Kryo>() {
        @Override
        protected Kryo initialValue() {
            Kryo kryo = new Kryo();
            Kryo.DefaultInstantiatorStrategy is = new Kryo.DefaultInstantiatorStrategy();
            is.setFallbackInstantiatorStrategy(new StdInstantiatorStrategy());
            kryo.setInstantiatorStrategy(is);

            kryo.getFieldSerializerConfig().setOptimizedGenerics(false);
            
            // Configure reference resolver to prevent IndexOutOfBoundsException
            // Disable references to avoid MapReferenceResolver issues
            kryo.setReferences(false);
            kryo.setRegistrationRequired(false);
            
            // Set additional safety configurations
            kryo.setAutoReset(false);
            kryo.setCopyReferences(false);
            
            // Set buffer size to prevent Buffer underflow
            kryo.getFieldSerializerConfig().setUseAsm(false);
            kryo.getFieldSerializerConfig().setUseUnsafe(false);

            kryo.register(Arrays.asList("").getClass(), new ArraysAsListSerializer());
            kryo.register(Collections.EMPTY_LIST.getClass(), new CollectionsEmptyListSerializer());
            kryo.register(Collections.singletonList("").getClass(),
                new CollectionsSingletonListSerializer());
            kryo.register(ClosureSerializer.Closure.class, new ClosureSerializer());

            ArrayListMultimapSerializer.registerSerializers(kryo);
            HashMultimapSerializer.registerSerializers(kryo);
            ImmutableListSerializer.registerSerializers(kryo);
            ImmutableMapSerializer.registerSerializers(kryo);
            ImmutableMultimapSerializer.registerSerializers(kryo);
            ImmutableSetSerializer.registerSerializers(kryo);
            ImmutableSortedSetSerializer.registerSerializers(kryo);
            LinkedHashMultimapSerializer.registerSerializers(kryo);
            LinkedListMultimapSerializer.registerSerializers(kryo);
            ReverseListSerializer.registerSerializers(kryo);
            TreeMultimapSerializer.registerSerializers(kryo);
            UnmodifiableNavigableSetSerializer.registerSerializers(kryo);
            SubListSerializers4Jdk9.addDefaultSerializers(kryo);
            UnmodifiableCollectionsSerializer.registerSerializers(kryo);

            ClassLoader tcl = Thread.currentThread().getContextClassLoader();
            if (tcl != null) {
                kryo.setClassLoader(tcl);
            }

            if (registeredSerializers != null) {
                for (Map.Entry<Class, Serializer> entry : registeredSerializers.entrySet()) {
                    LOGGER.info("register class:{} serializer", entry.getKey().getSimpleName());
                    kryo.register(entry.getKey(), entry.getValue());
                }
            }

            if (needRegisterClasses != null && needRegisterClasses.size() != 0) {
                for (String clazz : needRegisterClasses) {
                    String[] clazzToId = clazz.trim().split(":");
                    if (clazzToId.length != 2) {
                        throw new GeaflowRuntimeException("invalid clazzToId format:" + clazz);
                    }
                    int registerId = Integer.parseInt(clazzToId[1]);
                    registerClass(kryo, clazzToId[0], registerId);
                }
            }

            registerClass(kryo, "org.apache.geaflow.dsl.common.data.impl.BinaryRow", 1011);
            registerClass(kryo, "org.apache.geaflow.dsl.common.data.impl.DefaultParameterizedPath",
                "org.apache.geaflow.dsl.common.data.impl.DefaultParameterizedPath$DefaultParameterizedPathSerializer", 1012);
            registerClass(kryo, "org.apache.geaflow.dsl.common.data.impl.DefaultParameterizedRow",
                "org.apache.geaflow.dsl.common.data.impl.DefaultParameterizedRow$DefaultParameterizedRowSerializer", 1013);
            registerClass(kryo, "org.apache.geaflow.dsl.common.data.impl.DefaultPath",
                "org.apache.geaflow.dsl.common.data.impl.DefaultPath$DefaultPathSerializer", 1014);
            registerClass(kryo, "org.apache.geaflow.dsl.common.data.impl.DefaultRowKeyWithRequestId",
                "org.apache.geaflow.dsl.common.data.impl.DefaultRowKeyWithRequestId$DefaultRowKeyWithRequestIdSerializer", 1015);
            registerClass(kryo, "org.apache.geaflow.dsl.common.data.impl.ObjectRow",
                "org.apache.geaflow.dsl.common.data.impl.ObjectRow$ObjectRowSerializer", 1016);
            registerClass(kryo, "org.apache.geaflow.dsl.common.data.impl.ObjectRowKey",
                "org.apache.geaflow.dsl.common.data.impl.ObjectRowKey$ObjectRowKeySerializer", 1017);

            registerClass(kryo, "org.apache.geaflow.dsl.common.data.impl.types.BinaryStringEdge", 1018);
            registerClass(kryo, "org.apache.geaflow.dsl.common.data.impl.types.BinaryStringTsEdge", 1019);
            registerClass(kryo, "org.apache.geaflow.dsl.common.data.impl.types.BinaryStringVertex", 1020);
            registerClass(kryo, "org.apache.geaflow.dsl.common.data.impl.types.DoubleEdge", 1021);
            registerClass(kryo, "org.apache.geaflow.dsl.common.data.impl.types.DoubleTsEdge", 1022);
            registerClass(kryo, "org.apache.geaflow.dsl.common.data.impl.types.DoubleVertex", 1023);
            registerClass(kryo, "org.apache.geaflow.dsl.common.data.impl.types.IntEdge", 1024);
            registerClass(kryo, "org.apache.geaflow.dsl.common.data.impl.types.IntTsEdge", 1025);
            registerClass(kryo, "org.apache.geaflow.dsl.common.data.impl.types.IntVertex", 1026);
            registerClass(kryo, "org.apache.geaflow.dsl.common.data.impl.types.LongEdge", 1027);
            registerClass(kryo, "org.apache.geaflow.dsl.common.data.impl.types.LongTsEdge", 1028);
            registerClass(kryo, "org.apache.geaflow.dsl.common.data.impl.types.LongVertex", 1029);
            registerClass(kryo, "org.apache.geaflow.dsl.common.data.impl.types.ObjectEdge", 1030);
            registerClass(kryo, "org.apache.geaflow.dsl.common.data.impl.types.ObjectTsEdge", 1031);
            registerClass(kryo, "org.apache.geaflow.dsl.common.data.impl.types.ObjectVertex", 1032);

            registerClass(kryo, "org.apache.geaflow.dsl.runtime.traversal.data.FieldAlignEdge",
                "org.apache.geaflow.dsl.runtime.traversal.data.FieldAlignEdge$FieldAlignEdgeSerializer", 1033);
            registerClass(kryo, "org.apache.geaflow.dsl.runtime.traversal.data.FieldAlignPath",
                "org.apache.geaflow.dsl.runtime.traversal.data.FieldAlignPath$FieldAlignPathSerializer", 1034);
            registerClass(kryo, "org.apache.geaflow.dsl.runtime.traversal.data.FieldAlignVertex",
                "org.apache.geaflow.dsl.runtime.traversal.data.FieldAlignVertex$FieldAlignVertexSerializer", 1035);
            registerClass(kryo, "org.apache.geaflow.dsl.runtime.traversal.data.IdOnlyVertex", 1036);

            registerClass(kryo, "org.apache.geaflow.dsl.common.data.ParameterizedRow", 1037);
            registerClass(kryo, "org.apache.geaflow.dsl.common.data.Path", 1038);
            registerClass(kryo, "org.apache.geaflow.dsl.common.data.Row", 1039);
            registerClass(kryo, "org.apache.geaflow.dsl.common.data.RowEdge", 1040);
            registerClass(kryo, "org.apache.geaflow.dsl.common.data.RowKey", 1041);
            registerClass(kryo, "org.apache.geaflow.dsl.common.data.RowKeyWithRequestId", 1042);
            registerClass(kryo, "org.apache.geaflow.dsl.common.data.RowVertex", 1043);
            registerClass(kryo, "org.apache.geaflow.dsl.common.data.impl.ParameterizedPath", 1044);

            registerClass(kryo, "org.apache.geaflow.dsl.runtime.traversal.message.EODMessage",
                "org.apache.geaflow.dsl.runtime.traversal.message.EODMessage$EODMessageSerializer", 1045);
            registerClass(kryo, "org.apache.geaflow.dsl.runtime.traversal.message.IPathMessage", 1046);
            registerClass(kryo, "org.apache.geaflow.dsl.runtime.traversal.message.JoinPathMessage",
                "org.apache.geaflow.dsl.runtime.traversal.message.JoinPathMessage$JoinPathMessageSerializer", 1047);
            registerClass(kryo, "org.apache.geaflow.dsl.runtime.traversal.message.KeyGroupMessage", 1048);
            registerClass(kryo, "org.apache.geaflow.dsl.runtime.traversal.message.KeyGroupMessageImpl",
                "org.apache.geaflow.dsl.runtime.traversal.message.KeyGroupMessageImpl$KeyGroupMessageImplSerializer", 1049);
            registerClass(kryo, "org.apache.geaflow.dsl.runtime.traversal.message.ParameterRequestMessage",
                "org.apache.geaflow.dsl.runtime.traversal.message.ParameterRequestMessage$ParameterRequestMessageSerializer", 1050);
            registerClass(kryo, "org.apache.geaflow.dsl.runtime.traversal.message.RequestIsolationMessage", 1051);
            registerClass(kryo, "org.apache.geaflow.dsl.runtime.traversal.message.ReturnMessage", 1052);
            registerClass(kryo, "org.apache.geaflow.dsl.runtime.traversal.message.ReturnMessageImpl",
                "org.apache.geaflow.dsl.runtime.traversal.message.ReturnMessageImpl$ReturnMessageImplSerializer", 1053);

            registerClass(kryo, "org.apache.geaflow.dsl.runtime.traversal.path.AbstractSingleTreePath", 1054);
            registerClass(kryo, "org.apache.geaflow.dsl.runtime.traversal.path.AbstractTreePath", 1055);
            registerClass(kryo, "org.apache.geaflow.dsl.runtime.traversal.path.EdgeTreePath",
                "org.apache.geaflow.dsl.runtime.traversal.path.EdgeTreePath$EdgeTreePathSerializer", 1056);
            registerClass(kryo, "org.apache.geaflow.dsl.runtime.traversal.path.EmptyTreePath", 1057);
            registerClass(kryo, "org.apache.geaflow.dsl.runtime.traversal.path.ITreePath", 1058);
            registerClass(kryo, "org.apache.geaflow.dsl.runtime.traversal.path.ParameterizedTreePath",
                "org.apache.geaflow.dsl.runtime.traversal.path.ParameterizedTreePath$ParameterizedTreePathSerializer", 1059);
            registerClass(kryo, "org.apache.geaflow.dsl.runtime.traversal.path.SourceEdgeTreePath",
                "org.apache.geaflow.dsl.runtime.traversal.path.SourceEdgeTreePath$SourceEdgeTreePathSerializer", 1060);
            registerClass(kryo, "org.apache.geaflow.dsl.runtime.traversal.path.SourceVertexTreePath",
                "org.apache.geaflow.dsl.runtime.traversal.path.SourceVertexTreePath$SourceVertexTreePathSerializer", 1061);
            registerClass(kryo, "org.apache.geaflow.dsl.runtime.traversal.path.UnionTreePath",
                "org.apache.geaflow.dsl.runtime.traversal.path.UnionTreePath$UnionTreePathSerializer", 1062);
            registerClass(kryo, "org.apache.geaflow.dsl.runtime.traversal.path.VertexTreePath",
                "org.apache.geaflow.dsl.runtime.traversal.path.VertexTreePath$VertexTreePathSerializer", 1063);

            // Register MST algorithm related classes
            registerClass(kryo, "org.apache.geaflow.dsl.udf.graph.mst.MSTMessage", 1064);
            registerClass(kryo, "org.apache.geaflow.dsl.udf.graph.mst.MSTVertexState", 1065);
            registerClass(kryo, "org.apache.geaflow.dsl.udf.graph.mst.MSTEdge", 1066);
            registerClass(kryo, "org.apache.geaflow.dsl.udf.graph.mst.MSTMessage$MessageType", 1067);

            // Register binary object related classes
            registerClass(kryo, "org.apache.geaflow.common.binary.IBinaryObject", 106);
            registerClass(kryo, "org.apache.geaflow.common.binary.HeapBinaryObject", 112);

            return kryo;
        }
    };

    private void registerClass(Kryo kryo, String className, int kryoId) {
        try {
            LOGGER.debug("register class:{} id:{}", className, kryoId);
            Class<?> clazz = ClassUtil.classForName(className);
            kryo.register(clazz, kryoId);
        } catch (GeaflowRuntimeException e) {
            if (e.getCause() instanceof ClassNotFoundException) {
                LOGGER.warn("class not found: {} skip register id:{}", className, kryoId);
            }
        } catch (Throwable e) {
            LOGGER.error("error in register class: {} to kryo.", className);
            throw new GeaflowRuntimeException(e);
        }
    }

    private void registerClass(Kryo kryo, String className, String serializerClassName, int kryoId) {
        try {
            LOGGER.debug("register class:{} id:{}", className, kryoId);
            Class<?> clazz = ClassUtil.classForName(className);
            Class<?> serializerClazz = ClassUtil.classForName(serializerClassName);
            Serializer serializer = (Serializer) serializerClazz.newInstance();
            kryo.register(clazz, serializer, kryoId);
        } catch (GeaflowRuntimeException e) {
            if (e.getCause() instanceof ClassNotFoundException) {
                LOGGER.warn("class not found: {} skip register id:{}", className, kryoId);
            }
        } catch (Throwable e) {
            LOGGER.error("error in register class: {} to kryo.", className);
            throw new GeaflowRuntimeException(e);
        }
    }

    @Override
    public byte[] serialize(Object o) {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream(INITIAL_BUFFER_SIZE);
        Output output = new Output(outputStream);
        try {
            local.get().writeClassAndObject(output, o);
            output.flush();
        } finally {
            output.clear();
            output.close();
        }
        return outputStream.toByteArray();
    }

    @Override
    public Object deserialize(byte[] bytes) {
        if (bytes == null || bytes.length == 0) {
            throw new IllegalArgumentException("Cannot deserialize null or empty byte array");
        }
        
        try {
            // Use a more conservative approach with proper buffer sizing
            Input input = new Input(bytes);
            // Set buffer size to be at least as large as the data, but not too large to avoid overflow
            int bufferSize = Math.max(bytes.length, INITIAL_BUFFER_SIZE);
            input.setBuffer(bytes, 0, bufferSize);
            return local.get().readClassAndObject(input);
        } catch (Exception e) {
            // If deserialization fails, try with a larger buffer
            try {
                Input input = new Input(bytes);
                input.setBuffer(bytes, 0, bytes.length * 2);
                return local.get().readClassAndObject(input);
            } catch (Exception e2) {
                throw new RuntimeException("Failed to deserialize byte array after retry", e2);
            }
        }
    }

    @Override
    public void serialize(Object o, OutputStream outputStream) {
        Output output = new Output(outputStream);
        try {
            local.get().writeClassAndObject(output, o);
            output.flush();
        } finally {
            output.clear();
            output.close();
        }
    }

    @Override
    public Object deserialize(InputStream inputStream) {
        try {
            // First check if the input stream is available and has data
            if (inputStream == null) {
                throw new IllegalArgumentException("Input stream is null");
            }
            
            // Try to read a small amount to check if stream has data
            inputStream.mark(1);
            int firstByte = inputStream.read();
            if (firstByte == -1) {
                throw new IllegalArgumentException("Input stream is empty");
            }
            inputStream.reset();
            
            // Use a more conservative approach with smaller buffer and error handling
            Input input = new Input(inputStream, INITIAL_BUFFER_SIZE); // Use default 64KB buffer
            return local.get().readClassAndObject(input);
        } catch (Exception e) {
            // If the first attempt fails, try reading all bytes first
            try {
                if (inputStream == null) {
                    throw new IllegalArgumentException("Input stream is null");
                }
                
                // Reset the stream if possible
                try {
                    inputStream.reset();
                } catch (Exception resetException) {
                    // Stream doesn't support reset, continue with reading
                }
                
                byte[] buffer = new byte[4096]; // 4KB buffer
                java.io.ByteArrayOutputStream baos = new java.io.ByteArrayOutputStream();
                int bytesRead;
                int totalBytes = 0;
                while ((bytesRead = inputStream.read(buffer)) != -1) {
                    baos.write(buffer, 0, bytesRead);
                    totalBytes += bytesRead;
                }
                
                if (totalBytes == 0) {
                    throw new IllegalArgumentException("Input stream is empty");
                }
                
                byte[] allBytes = baos.toByteArray();
                
                // Use the byte array deserialize method
                return deserialize(allBytes);
            } catch (Exception e2) {
                throw new RuntimeException("Failed to deserialize from InputStream after retry", e2);
            }
        }
    }

    public Kryo getThreadKryo() {
        return local.get();
    }

    @Override
    public <T> T copy(T target) {
        return local.get().copy(target);
    }

    public void clean() {
        local.remove();
    }
}
