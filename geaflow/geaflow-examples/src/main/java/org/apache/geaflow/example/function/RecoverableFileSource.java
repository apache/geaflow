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

package org.apache.geaflow.example.function;

import static org.apache.geaflow.common.config.keys.FrameworkConfigKeys.BATCH_NUMBER_PER_CHECKPOINT;
import static org.apache.geaflow.common.config.keys.FrameworkConfigKeys.SYSTEM_OFFSET_BACKEND_TYPE;

import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.geaflow.api.context.RuntimeContext;
import org.apache.geaflow.api.window.IWindow;
import org.apache.geaflow.common.config.keys.ExecutionConfigKeys;
import org.apache.geaflow.common.exception.GeaflowRuntimeException;
import org.apache.geaflow.common.serialize.ISerializer;
import org.apache.geaflow.common.serialize.SerializerFactory;
import org.apache.geaflow.common.utils.CheckpointUtil;
import org.apache.geaflow.state.KeyValueState;
import org.apache.geaflow.state.StateFactory;
import org.apache.geaflow.state.descriptor.KeyValueStateDescriptor;
import org.apache.geaflow.state.serializer.IKVSerializer;
import org.apache.geaflow.store.rocksdb.RocksdbStoreBuilder;
import org.apache.geaflow.utils.keygroup.IKeyGroupAssigner;
import org.apache.geaflow.utils.keygroup.KeyGroup;
import org.apache.geaflow.utils.keygroup.KeyGroupAssignment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RecoverableFileSource<OUT> extends FileSource<OUT> {

    private static final Logger LOGGER = LoggerFactory.getLogger(RecoverableFileSource.class);
    private static final Pattern P = Pattern.compile("(PipelineTask#\\d+).*(.*cycle#\\d+)-\\d+(.*)");

    private KeyValueState<Integer, Integer> offsetState;
    private long checkpointDuration;

    public RecoverableFileSource(String filePath, FileLineParser<OUT> parser) {
        super(filePath, parser);
    }

    @Override
    public void open(RuntimeContext runtimeContext) {
        super.open(runtimeContext);
        this.offsetState = OffsetKVStateKeeper.build(runtimeContext);
        this.checkpointDuration = this.runtimeContext.getConfiguration().getLong(BATCH_NUMBER_PER_CHECKPOINT);
        LOGGER.info("open file source, taskIndex {}, taskName {}, checkpointDuration {}",
            runtimeContext.getTaskArgs().getTaskId(), runtimeContext.getTaskArgs().getTaskName(), checkpointDuration);
    }

    @Override
    public boolean fetch(IWindow<OUT> window, SourceContext<OUT> ctx) throws Exception {
        if (window.windowId() == 1) {
            readPos = 0;
            LOGGER.info("init readPos to {} for windowId {}", 0, window.windowId());
        } else {
            if (readPos == null) {
                long lastWindowId = window.windowId() - 1;
                LOGGER.info("need recover readPos for last windowId {}", lastWindowId);
                offsetState.manage().operate().setCheckpointId(lastWindowId);
                offsetState.manage().operate().recover();
                Integer pos = offsetState.get(runtimeContext.getTaskArgs().getTaskIndex());
                if (pos != null) {
                    LOGGER.info("windowId{} recover readPos {}", window.windowId(), pos);
                    readPos = pos;
                } else {
                    LOGGER.info("not found readPos set to {}", 0);
                    readPos = 0;
                }
            } else {
                LOGGER.info("current windowId {} readPos {}", window.windowId(), readPos);
            }
        }
        boolean result = super.fetch(window, ctx);
        offsetState.put(runtimeContext.getTaskArgs().getTaskIndex(), readPos);

        long batchId = window.windowId();
        if (CheckpointUtil.needDoCheckpoint(batchId, checkpointDuration)) {
            offsetState.manage().operate().setCheckpointId(batchId);
            offsetState.manage().operate().finish();
            offsetState.manage().operate().archive();
            LOGGER.info("do checkpoint windowId {} readPos {}", batchId, readPos);
        }
        return result;
    }

    @Override
    public void close() {

    }

    static class OffsetKVStateKeeper {

        private static volatile Map<String, KeyValueState<Integer, Integer>> KV_STATE_MAP = new ConcurrentHashMap<>();

        public static KeyValueState<Integer, Integer> build(RuntimeContext runtimeContext) {
            // Pipeline task name pattern PipelineTask#0-[windowId] cycle#1-[iterationId],
            // e.g. PipelineTask#0-1 cycle#1-1.
            // We only extract name without any windowId or iterationId for store name.
            Matcher matcher = P.matcher(runtimeContext.getTaskArgs().getTaskName());
            String taskName;
            if (matcher.find()) {
                taskName = matcher.group(1) + matcher.group(2);
            } else {
                taskName = runtimeContext.getTaskArgs().getTaskName();
            }

            taskName = String.format("%s_%s_%s",
                runtimeContext.getConfiguration().getString(ExecutionConfigKeys.JOB_UNIQUE_ID),
                taskName, runtimeContext.getTaskArgs().getTaskId());
            if (KV_STATE_MAP.get(taskName) == null) {
                synchronized (OffsetKVStateKeeper.class) {
                    if (KV_STATE_MAP.get(taskName) == null) {
                        KeyValueStateDescriptor descriptor = KeyValueStateDescriptor.build(
                            taskName,
                            runtimeContext.getConfiguration().getString(SYSTEM_OFFSET_BACKEND_TYPE));
                        for (Entry<String, String> entry : runtimeContext.getConfiguration().getConfigMap().entrySet()) {
                            LOGGER.info("runtime key {} value {}", entry.getKey(), entry.getValue());
                        }
                        if (descriptor.getStoreType().equalsIgnoreCase(new RocksdbStoreBuilder().getStoreDesc().name())) {
                            throw new GeaflowRuntimeException("GeaFlow offset not support ROCKSDB storage and should "
                                + "be configured as JDBC or MEMORY");
                        }
                        int taskIndex = runtimeContext.getTaskArgs().getTaskIndex();
                        int maxParallelism = runtimeContext.getTaskArgs().getMaxParallelism();
                        KeyGroup keyGroup = KeyGroupAssignment.computeKeyGroupRangeForOperatorIndex(maxParallelism,
                            runtimeContext.getTaskArgs().getParallelism(),
                            taskIndex);
                        descriptor.withKeyGroup(keyGroup);
                        descriptor.withKVSerializer(new OffsetKvSerializer());
                        descriptor.withKeyGroupAssigner(new IntKeyGroupAssigner(maxParallelism));
                        KV_STATE_MAP.put(taskName, StateFactory.buildKeyValueState(descriptor, runtimeContext.getConfiguration()));
                    }
                }
            }
            return KV_STATE_MAP.get(taskName);
        }

    }

    static class IntKeyGroupAssigner implements IKeyGroupAssigner {

        private int maxPara;

        public IntKeyGroupAssigner(int maxPara) {
            this.maxPara = maxPara;
        }

        @Override
        public int getKeyGroupNumber() {
            return this.maxPara;
        }

        @Override
        public int assign(Object key) {
            if (key == null) {
                return -1;
            }
            return Math.abs(((int) key) % maxPara);
        }
    }

    static class OffsetKvSerializer implements IKVSerializer<Integer, Integer> {

        private final ISerializer serializer;

        public OffsetKvSerializer() {
            this.serializer = SerializerFactory.getKryoSerializer();
        }

        @Override
        public byte[] serializeKey(Integer key) {
            return serializer.serialize(key);
        }

        @Override
        public Integer deserializeKey(byte[] array) {
            if (array == null) {
                return null;
            }
            return (Integer) serializer.deserialize(array);
        }

        @Override
        public byte[] serializeValue(Integer value) {
            return serializer.serialize(value);
        }

        @Override
        public Integer deserializeValue(byte[] valueArray) {
            if (valueArray == null) {
                return null;
            }
            return (Integer) serializer.deserialize(valueArray);
        }
    }
}
