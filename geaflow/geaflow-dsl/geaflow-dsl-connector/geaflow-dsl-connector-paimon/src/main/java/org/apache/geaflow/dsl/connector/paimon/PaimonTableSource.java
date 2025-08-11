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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.commons.lang.StringUtils;
import org.apache.geaflow.api.context.RuntimeContext;
import org.apache.geaflow.api.window.WindowType;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.common.config.keys.DSLConfigKeys;
import org.apache.geaflow.common.utils.GsonUtil;
import org.apache.geaflow.dsl.common.exception.GeaFlowDSLException;
import org.apache.geaflow.dsl.common.types.TableSchema;
import org.apache.geaflow.dsl.common.util.Windows;
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
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.utils.CloseableIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PaimonTableSource implements TableSource {

    private static final Logger LOGGER = LoggerFactory.getLogger(PaimonTableSource.class);

    private Configuration tableConf;
    private TableSchema tableSchema;
    private boolean isAllWindow;

    private String path;
    private Map<String, String> options;
    private String configJson;
    private Map<String, String> configs;
    private String database;
    private String tableName;

    private transient CatalogContext catalogContext;
    private transient Catalog catalog;
    private transient ReadBuilder readBuilder;
    private transient Map<PaimonPartition, RecordReader<InternalRow>> partition2Reader;
    private transient Map<PaimonPartition, PaimonOffset> partition2InnerOffset;

    @Override
    public void init(Configuration tableConf, TableSchema tableSchema) {
        this.tableConf = tableConf;
        this.isAllWindow = tableConf.getLong(DSLConfigKeys.GEAFLOW_DSL_WINDOW_SIZE) == Windows.SIZE_OF_ALL_WINDOW;
        this.tableSchema = tableSchema;
        this.path = tableConf.getString(PaimonConfigKeys.GEAFLOW_DSL_PAIMON_WAREHOUSE, "");
        this.options = new HashMap<>();
        this.configs = new HashMap<>();
        if (StringUtils.isBlank(this.path)) {
            String optionJson = tableConf.getString(PaimonConfigKeys.GEAFLOW_DSL_PAIMON_OPTIONS_JSON);
            Map<String, String> userOptions = GsonUtil.parse(optionJson);
            if (userOptions != null) {
                for (Map.Entry<String, String> entry : userOptions.entrySet()) {
                    options.put(entry.getKey(), entry.getValue());
                }
            }
            this.configJson =
                tableConf.getString(PaimonConfigKeys.GEAFLOW_DSL_PAIMON_CONFIGURATION_JSON, "");
            if (!StringUtils.isBlank(configJson)) {
                Map<String, String> userConfig = GsonUtil.parse(configJson);
                if (userConfig != null) {
                    for (Map.Entry<String, String> entry : userConfig.entrySet()) {
                        configs.put(entry.getKey(), entry.getValue());
                    }
                }
            }
        }
        this.database = tableConf.getString(PaimonConfigKeys.GEAFLOW_DSL_PAIMON_DATABASE_NAME);
        this.tableName = tableConf.getString(PaimonConfigKeys.GEAFLOW_DSL_PAIMON_TABLE_NAME);
    }

    @Override
    public List<Partition> listPartitions(int parallelism) {
        return listPartitions();
    }

    @Override
    public void open(RuntimeContext context) {
        if (StringUtils.isBlank(this.path)) {
            if (StringUtils.isBlank(this.configJson)) {
                this.catalogContext =
                    Objects.requireNonNull(CatalogContext.create(new Options(options)));
            } else {
                org.apache.hadoop.conf.Configuration hadoopConf = new org.apache.hadoop.conf.Configuration();
                for (Map.Entry<String, String> entry : configs.entrySet()) {
                    hadoopConf.set(entry.getKey(), entry.getValue());
                }
                this.catalogContext =
                    Objects.requireNonNull(CatalogContext.create(new Options(options), hadoopConf));
            }
        } else {
            this.catalogContext = Objects.requireNonNull(CatalogContext.create(new Path(path)));
        }
        this.catalog = Objects.requireNonNull(CatalogFactory.createCatalog(this.catalogContext));
        Identifier identifier = Identifier.create(database, tableName);
        try {
            this.readBuilder = Objects.requireNonNull(catalog.getTable(identifier).newReadBuilder());
        } catch (TableNotExistException e) {
            throw new GeaFlowDSLException("Table: {} in db: {} not exists.", tableName, database);
        }
        this.partition2Reader = new HashMap<>();
        this.partition2InnerOffset = new HashMap<>();
        LOGGER.info("Open paimon source, tableConf: {}, tableSchema: {}, path: {}, options: "
            + "{}, configs: {}, database: {}, tableName: {}", tableConf, tableSchema, path,
            options, configs, database, tableName);
    }

    @Override
    public List<Partition> listPartitions() {
        List<Split> splits = isAllWindow ? readBuilder.newScan().plan().splits() :
                             readBuilder.newStreamScan().plan().splits();
        return splits.stream().map(split -> new PaimonPartition(database, tableName, split)).collect(Collectors.toList());
    }

    @Override
    public <IN> TableDeserializer<IN> getDeserializer(Configuration conf) {
        return (TableDeserializer<IN>) new PaimonRecordDeserializer();
    }

    @Override
    public <T> FetchData fetch(Partition partition, Optional<Offset> startOffset, FetchWindow windowInfo)
        throws IOException {
        PaimonPartition paimonPartition = (PaimonPartition) partition;
        assert paimonPartition.getDatabase().equals(this.database)
            && paimonPartition.getTable().equals(this.tableName);
        RecordReader reader = partition2Reader.getOrDefault(partition,
            readBuilder.newRead().createReader(paimonPartition.getSplit()));
        partition2Reader.put(paimonPartition, reader);

        PaimonOffset innerOffset = partition2InnerOffset.getOrDefault(partition,
            new PaimonOffset());
        partition2InnerOffset.put(paimonPartition, innerOffset);

        if (startOffset.isPresent() && !startOffset.get().equals(innerOffset)) {
            throw new GeaFlowDSLException("Paimon connector not support reset offset.");
        }
        CloseableIterator iterator = reader.toCloseableIterator();
        if (windowInfo.getType() == WindowType.ALL_WINDOW) {
            return FetchData.createBatchFetch(iterator, new PaimonOffset());
        } else if (windowInfo.getType() == WindowType.SIZE_TUMBLING_WINDOW) {
            List<Object> readContents = new ArrayList<>();
            long i = 0;
            for (; i < windowInfo.windowSize(); i++) {
                if (iterator.hasNext()) {
                    readContents.add(iterator.next());
                } else {
                    break;
                }
            }
            long nextOffset = innerOffset.getOffset() + i;
            boolean isFinished = !iterator.hasNext();
            return FetchData.createStreamFetch(readContents, new PaimonOffset(nextOffset), isFinished);
        } else {
            throw new GeaFlowDSLException("Paimon not support window:{}", windowInfo.getType());
        }
    }

    @Override
    public void close() {
        for (RecordReader reader : partition2Reader.values()) {
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException e) {
                    throw new GeaFlowDSLException("Error occurs when close paimon reader.", e);
                }
            }
        }
        partition2Reader.clear();
        partition2InnerOffset.clear();
    }

    public static class PaimonPartition implements Partition {

        private final String database;
        private final String table;
        private final Split split;

        public PaimonPartition(String database, String table, Split split) {
            this.database = Objects.requireNonNull(database);
            this.table = Objects.requireNonNull(table);
            this.split = Objects.requireNonNull(split);
        }

        @Override
        public String getName() {
            return database + "-" + table;
        }

        @Override
        public int hashCode() {
            return Objects.hash(database, table, split);
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
                split, that.split);
        }

        public String getDatabase() {
            return database;
        }

        public String getTable() {
            return table;
        }

        public Split getSplit() {
            return split;
        }

        @Override
        public void setIndex(int index, int parallel) {
        }
    }


    public static class PaimonOffset implements Offset {

        private final long offset;


        public PaimonOffset() {
            this.offset = 0L;
        }

        public PaimonOffset(long offset) {
            this.offset = offset;
        }

        @Override
        public String humanReadable() {
            return String.valueOf(offset);
        }

        @Override
        public long getOffset() {
            return offset;
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
            return offset == that.offset;
        }

        @Override
        public int hashCode() {
            return Objects.hash(offset);
        }
    }
}
