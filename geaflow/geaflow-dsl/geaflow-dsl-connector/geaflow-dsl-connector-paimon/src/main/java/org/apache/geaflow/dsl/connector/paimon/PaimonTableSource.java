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

package org.apache.geaflow.dsl.connector.paimon;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import org.apache.commons.lang.StringUtils;
import org.apache.geaflow.api.context.RuntimeContext;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.common.utils.GsonUtil;
import org.apache.geaflow.dsl.common.exception.GeaFlowDSLException;
import org.apache.geaflow.dsl.common.types.TableSchema;
import org.apache.geaflow.dsl.connector.api.FetchData;
import org.apache.geaflow.dsl.connector.api.Offset;
import org.apache.geaflow.dsl.connector.api.Partition;
import org.apache.geaflow.dsl.connector.api.TableSource;
import org.apache.geaflow.dsl.connector.api.serde.TableDeserializer;
import org.apache.geaflow.dsl.connector.api.window.FetchWindow;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Catalog.TableNotExistException;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.fs.Path;
import org.apache.paimon.options.Options;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.source.StreamTableScan;
import org.apache.paimon.utils.CloseableIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Paimon table source. */
public class PaimonTableSource implements TableSource {

    private static final Logger LOGGER = LoggerFactory.getLogger(PaimonTableSource.class);

    private Configuration tableConf;
    private TableSchema tableSchema;

    private String path;
    private Map<String, String> options;
    private String configJson;
    private Map<String, String> configs;
    private String database;
    private String table;
    private SourceMode sourceMode;
    private final PaimonRecordDeserializer deserializer = new PaimonRecordDeserializer();

    private transient long fromSnapshot;
    private transient ReadBuilder readBuilder;
    private transient Map<Split, CloseableIterator<InternalRow>> iterators;
    private transient Map<Split, RecordReader<InternalRow>> readers;
    private transient Map<PaimonPartition, PaimonOffset> offsets;

    @Override
    public void init(Configuration tableConf, TableSchema tableSchema) {
        this.tableConf = tableConf;
        this.tableSchema = tableSchema;
        this.path = tableConf.getString(PaimonConfigKeys.GEAFLOW_DSL_PAIMON_WAREHOUSE, "");
        this.options = new HashMap<>();
        this.configs = new HashMap<>();
        if (StringUtils.isBlank(this.path)) {
            String optionJson = tableConf.getString(PaimonConfigKeys.GEAFLOW_DSL_PAIMON_OPTIONS_JSON);
            Map<String, String> userOptions = GsonUtil.parse(optionJson);
            if (userOptions != null) {
                options.putAll(userOptions);
            }
            this.configJson =
                tableConf.getString(PaimonConfigKeys.GEAFLOW_DSL_PAIMON_CONFIGURATION_JSON, "");
            if (!StringUtils.isBlank(configJson)) {
                Map<String, String> userConfig = GsonUtil.parse(configJson);
                if (userConfig != null) {
                    configs.putAll(userConfig);
                }
            }
        }
        this.database = tableConf.getString(PaimonConfigKeys.GEAFLOW_DSL_PAIMON_DATABASE_NAME);
        this.table = tableConf.getString(PaimonConfigKeys.GEAFLOW_DSL_PAIMON_TABLE_NAME);
        this.sourceMode = SourceMode.valueOf(tableConf.getString(PaimonConfigKeys.GEAFLOW_DSL_PAIMON_SOURCE_MODE).toUpperCase());
    }

    @Override
    public List<Partition> listPartitions(int parallelism) {
        List<Partition> partitions = new ArrayList<>(parallelism);
        for (int i = 0; i < parallelism; i++) {
            partitions.add(new PaimonPartition(database, table));
        }
        return partitions;
    }

    @Override
    public List<Partition> listPartitions() {
        throw new UnsupportedOperationException("Please use listPartitions(int parallelism) instead");
    }

    @Override
    public void open(RuntimeContext context) {
        Catalog catalog = getPaimonCatalog();
        Identifier identifier = Identifier.create(database, table);
        try {
            this.readBuilder = Objects.requireNonNull(catalog.getTable(identifier).newReadBuilder());
            if (tableConf.getString(PaimonConfigKeys.GEAFLOW_DSL_PAIMON_SCAN_MODE)
                    .equalsIgnoreCase(StartupMode.FROM_SNAPSHOT.getValue())
                && tableConf.contains(PaimonConfigKeys.GEAFLOW_DSL_PAIMON_SCAN_SNAPSHOT_ID)) {
                this.fromSnapshot = tableConf.getLong(PaimonConfigKeys.GEAFLOW_DSL_PAIMON_SCAN_SNAPSHOT_ID);
            } else {
                this.fromSnapshot = catalog.getTable(identifier).latestSnapshotId().orElse(1L);
            }
            LOGGER.info("New partition will start from snapshot: {}", this.fromSnapshot);
        } catch (TableNotExistException e) {
            throw new GeaFlowDSLException("Table: {} in db: {} not exists.", table, database);
        }
        this.iterators = new HashMap<>();
        this.readers = new HashMap<>();
        this.offsets = new HashMap<>();
        LOGGER.info("Open paimon source, tableConf: {}, tableSchema: {}, path: {}, options: "
            + "{}, configs: {}, database: {}, tableName: {}", tableConf, tableSchema, path,
            options, configs, database, table);
        this.deserializer.init(tableSchema);
    }

    @Override
    public <IN> TableDeserializer<IN> getDeserializer(Configuration conf) {
        return null;
    }

    @Override
    public <T> FetchData fetch(Partition partition, Optional<Offset> startOffset, FetchWindow windowInfo)
        throws IOException {
        PaimonPartition paimonPartition = (PaimonPartition) partition;
        assert paimonPartition.getDatabase().equals(this.database)
            && paimonPartition.getTable().equals(this.table);

        long skip = 0;
        PaimonOffset innerOffset = offsets.get(partition);
        if (innerOffset == null) {
            // first fetch, use custom specified snapshot id
            innerOffset = new PaimonOffset(fromSnapshot);
        }

        // if startOffset is specified, use it and try reset innerOffset
        if ((startOffset.isPresent() && !startOffset.get().equals(innerOffset))) {
            skip = Math.abs(startOffset.get().getOffset() - innerOffset.getOffset());
            innerOffset = PaimonOffset.from((PaimonOffset) startOffset.get());
        }
        if (paimonPartition.getCurrentSnapshot() != innerOffset.getSnapshotId()) {
            paimonPartition.reset(loadSplitsFrom(innerOffset.getSnapshotId()));
        }

        Split split = paimonPartition.seek(innerOffset.getSplitIndex());
        if (split == null) {
            if (sourceMode == SourceMode.BATCH) {
                LOGGER.info("No more split to fetch");
                return FetchData.createBatchFetch(Collections.emptyIterator(), innerOffset);
            } else {
                LOGGER.debug("Snapshot {} not ready now", innerOffset.getSnapshotId());
                return FetchData.createStreamFetch(Collections.emptyList(), innerOffset, false);
            }
        }
        offsets.put(paimonPartition, innerOffset);

        CloseableIterator<InternalRow> iterator = createRecordIterator(split);
        if (skip > 0) {
            while (iterator.hasNext() && skip > 0) {
                iterator.next();
                skip--;
            }
        }
        switch (windowInfo.getType()) {
            case ALL_WINDOW:
                return FetchData.createBatchFetch(new IteratorWrapper(iterator, deserializer), new PaimonOffset());
            case SIZE_TUMBLING_WINDOW:
                List<Object> readContents = new ArrayList<>();
                long advance = 0;
                for (long i = 0; i < windowInfo.windowSize(); i++) {
                    if (iterator.hasNext()) {
                        advance++;
                        readContents.add(deserializer.deserialize(iterator.next()));
                    } else {
                        if (sourceMode == SourceMode.BATCH) {
                            break;
                        }
                        try {
                            removeRecordIterator(split);
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                        innerOffset = innerOffset.nextSplit();
                        Split seek = paimonPartition.seek(innerOffset.getSplitIndex());
                        if (seek == null) {
                            // all split finished, try read next snapshot
                            innerOffset = innerOffset.nextSnapshot();
                            paimonPartition.reset(loadSplitsFrom(innerOffset.getSnapshotId()));
                        }
                        offsets.put(paimonPartition, innerOffset);
                        advance = 0L;
                        seek = paimonPartition.seek(innerOffset.getSplitIndex());
                        if (seek == null) {
                            // no new snapshot discovered, retry next turn
                            break;
                        }
                        iterator = createRecordIterator(seek);
                        i--;
                    }
                }
                PaimonOffset next = innerOffset.advance(advance);
                offsets.put(paimonPartition, next);
                boolean isFinished = sourceMode != SourceMode.STREAM && !iterator.hasNext();
                return FetchData.createStreamFetch(readContents, next, isFinished);
            default:
                throw new GeaFlowDSLException("Paimon not support window:{}", windowInfo.getType());
        }
    }

    /**
     * Create a paimon record iterator.
     * @param split the paimon data split
     * @return a paimon record iterator
     * @throws IOException if error occurs
     */
    private CloseableIterator<InternalRow> createRecordIterator(Split split) throws IOException {
        CloseableIterator<InternalRow> iterator = iterators.get(split);
        if (iterator != null) {
            return iterator;
        }
        RecordReader<InternalRow> reader = readBuilder.newRead().createReader(split);
        CloseableIterator<InternalRow> closeableIterator = reader.toCloseableIterator();
        readers.put(split, reader);
        iterators.put(split, closeableIterator);
        return closeableIterator;
    }

    /**
     * Remove the paimon record iterator and reader.
     * @param split the paimon data split
     * @throws Exception if error occurs
     */
    private void removeRecordIterator(Split split) throws Exception {
        CloseableIterator<InternalRow> removed = iterators.remove(split);
        if (removed != null) {
            removed.close();
        }
        RecordReader<InternalRow> reader = readers.remove(split);
        if (reader != null) {
            reader.close();
        }
    }

    /**
     * Load splits from snapshot.
     * @param snapshotId the snapshot id
     * @return all splits
     */
    private List<Split> loadSplitsFrom(long snapshotId) {
        StreamTableScan streamTableScan = readBuilder.newStreamScan();
        streamTableScan.restore(snapshotId);
        long start = System.currentTimeMillis();
        List<Split> splits = streamTableScan.plan().splits();
        LOGGER.debug("Load splits from snapshot: {}, cost: {}ms", snapshotId, System.currentTimeMillis() - start);
        return splits;
    }

    /**
     * Get paimon catalog.
     * @return the paimon catalog.
     */
    private Catalog getPaimonCatalog() {
        CatalogContext catalogContext;
        if (StringUtils.isBlank(this.path)) {
            if (StringUtils.isBlank(this.configJson)) {
                catalogContext =
                        Objects.requireNonNull(CatalogContext.create(new Options(options)));
            } else {
                org.apache.hadoop.conf.Configuration hadoopConf = new org.apache.hadoop.conf.Configuration();
                for (Map.Entry<String, String> entry : configs.entrySet()) {
                    hadoopConf.set(entry.getKey(), entry.getValue());
                }
                catalogContext =
                        Objects.requireNonNull(CatalogContext.create(new Options(options), hadoopConf));
            }
        } else {
            catalogContext = Objects.requireNonNull(CatalogContext.create(new Path(path)));
        }
        return Objects.requireNonNull(CatalogFactory.createCatalog(catalogContext));
    }

    @Override
    public void close() {
        for (CloseableIterator<InternalRow> reader : iterators.values()) {
            if (reader != null) {
                try {
                    reader.close();
                } catch (Exception e) {
                    throw new GeaFlowDSLException("Error occurs when close paimon iterator.", e);
                }
            }
        }
        for ( RecordReader<InternalRow> reader : readers.values()) {
            if (reader != null) {
                try {
                    reader.close();
                } catch (Exception e) {
                    throw new GeaFlowDSLException("Error occurs when close paimon reader.", e);
                }
            }
        }
        iterators.clear();
        offsets.clear();
    }

    public static class PaimonPartition implements Partition {

        private final String database;
        private final String table;

        private int index;
        private int parallel;

        // current snapshot id
        private transient long currentSnapshot;
        // assigned splits for this partition
        private final transient List<Split> splits;

        public PaimonPartition(String database, String table) {
            this.database = Objects.requireNonNull(database);
            this.table = Objects.requireNonNull(table);
            this.splits = new ArrayList<>();
            this.currentSnapshot = -1L;
            this.index = 0;
            this.parallel = 1;
        }

        public void reset(List<Split> splits) {
            this.splits.clear();
            for (Split split : splits) {
                DataSplit dataSplit = (DataSplit) split;
                int hash = Objects.hash(dataSplit.bucket(), dataSplit.partition());
                if (hash % parallel == index) {
                    this.splits.add(split);
                    this.currentSnapshot = dataSplit.snapshotId();
                }
            }
            if (!this.splits.isEmpty()) {
                LOGGER.info("Assign paimon split(s) for table {}.{}, snapshot: {}, split size: {}",
                        database, table, this.currentSnapshot, this.splits.size());
            }
        }

        @Override
        public String getName() {
            return database + "-" + table;
        }

        public String getDatabase() {
            return database;
        }

        public String getTable() {
            return table;
        }

        public long getCurrentSnapshot() {
            return currentSnapshot;
        }

        /**
         * Seek the split by index.
         * @param splitIndex the split index
         * @return the split to read
         */
        public Split seek(int splitIndex) {
            if (splitIndex >= splits.size()) {
                return null;
            }
            return splits.get(splitIndex);
        }

        @Override
        public void setIndex(int index, int parallel) {
            this.parallel = parallel;
            this.index = index;
        }

        @Override
        public int hashCode() {
            return Objects.hash(database, table, index);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof PaimonPartition)) {
                return false;
            }
            PaimonPartition that = (PaimonPartition) o;
            return Objects.equals(database, that.database) && Objects.equals(
                    table, that.table) && Objects.equals(
                    index, that.index);
        }
    }

    public static class PaimonOffset implements Offset {

        // if partition snapshot id is not equal to current snapshot id,
        // need to load splits
        private final long snapshotId;
        // the index of current partition assigned splits
        private final int splitIndex;
        // the offset of current split
        private final long offset;

        public static PaimonOffset from(PaimonOffset offset) {
            return new PaimonOffset(offset.snapshotId, offset.splitIndex, offset.offset);
        }

        public PaimonOffset() {
            this(1L);
        }

        public PaimonOffset(long snapshotId) {
            this(snapshotId, 0, 0);
        }

        public PaimonOffset(long snapshotId, int splitIndex, long offset) {
            this.snapshotId = snapshotId;
            this.splitIndex = splitIndex;
            this.offset = offset;
        }

        @Override
        public String humanReadable() {
            return String.format("snapshot %d, split index %d, offset %d", snapshotId, splitIndex, offset);
        }

        public long getSnapshotId() {
            return snapshotId;
        }

        public int getSplitIndex() {
            return splitIndex;
        }

        @Override
        public long getOffset() {
            return offset;
        }

        public PaimonOffset advance(long rows) {
            if (rows == 0) {
                return this;
            }
            return new PaimonOffset(snapshotId, splitIndex, offset + rows);
        }

        public PaimonOffset nextSplit() {
            return new PaimonOffset(snapshotId, splitIndex + 1, 0);
        }

        public PaimonOffset nextSnapshot() {
            return new PaimonOffset(snapshotId + 1, 0, 0);
        }

        @Override
        public boolean isTimestamp() {
            return false;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            PaimonOffset that = (PaimonOffset) o;
            return snapshotId == that.snapshotId && splitIndex == that.splitIndex && offset == that.offset;
        }

        @Override
        public int hashCode() {
            return Objects.hash(snapshotId, splitIndex, offset);
        }
    }
}
