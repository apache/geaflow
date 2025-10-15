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

package org.apache.geaflow.store.paimon;

import static org.testng.Assert.assertTrue;

import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.common.config.keys.ExecutionConfigKeys;
import org.apache.geaflow.store.context.StoreContext;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.reader.RecordReaderIterator;
import org.testng.annotations.Test;

public class PaimonRWHandleTest {

    static class GraphPaimonStoreTest extends BasePaimonStore {

        public void init(StoreContext storeContext) {
            super.init(storeContext);
        }

        @Override
        public void flush() {

        }

        @Override
        public void archive(long checkpointId) {

        }

        @Override
        public void recovery(long checkpointId) {

        }

        @Override
        public long recoveryLatest() {
            return 0;
        }

        @Override
        public void compact() {

        }

        @Override
        public void drop() {
            super.drop();
        }
    }

    @Test
    public void testGraphStore() throws Exception {
        // create edge table.
        GraphPaimonStoreTest storeBase = new GraphPaimonStoreTest();
        Configuration config = new Configuration();
        config.put(ExecutionConfigKeys.JOB_APP_NAME, "test_paimon_app");
        storeBase.init(new StoreContext("test_paimon_store").withConfig(config));

        PaimonTableRWHandle edgeHandle = storeBase.createEdgeTableHandle(
            new Identifier(storeBase.paimonStoreName, "edge"));

        // 写入一条数据 - 根据 schema 定义构造完整的行数据
        String srcId = "src1";
        String targetId = "dst1";
        long timestamp = System.currentTimeMillis();
        short direction = 1; // 假设 1 表示出边
        String label = "knows";
        String value = "edge-value-123";

        // 按照 schema 顺序构造 row: src_id, target_id, ts, direction, label, value
        GenericRow row = GenericRow.of(srcId.getBytes(),      // src_id (主键)
            targetId.getBytes(),   // target_id
            timestamp,             // ts
            direction,             // direction
            label.getBytes(),      // label
            value.getBytes()       // value
        );

        edgeHandle.write(row);
        long checkpointId = 1L;
        edgeHandle.commit(checkpointId);

        // 读取数据并断言 - 使用所有列的投影
        int[] projection = new int[]{0, 1, 2, 3, 4, 5}; // 所有列
        RecordReaderIterator<InternalRow> iterator = edgeHandle.getIterator(null, null, projection);
        boolean found = false;
        while (iterator.hasNext()) {
            InternalRow internalRow = iterator.next();
            String readSrcId = new String(internalRow.getBinary(0));
            String readTargetId = new String(internalRow.getBinary(1));
            long readTs = internalRow.getLong(2);
            short readDirection = internalRow.getShort(3);
            String readLabel = new String(internalRow.getBinary(4));
            String readValue = new String(internalRow.getBinary(5));

            if (srcId.equals(readSrcId) && targetId.equals(readTargetId) && timestamp == readTs
                && direction == readDirection && label.equals(readLabel) && value.equals(
                readValue)) {
                found = true;
                break;
            }
        }
        iterator.close();
        assertTrue(found);

        storeBase.drop();
        storeBase.close();
    }
}
