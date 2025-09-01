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

package org.apache.geaflow.dsl.connector.api;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.Optional;
import org.apache.geaflow.api.context.RuntimeContext;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.dsl.common.data.Row;
import org.apache.geaflow.dsl.common.types.TableSchema;
import org.apache.geaflow.dsl.connector.api.serde.TableDeserializer;
import org.apache.geaflow.dsl.connector.api.window.FetchWindow;

/**
 * Interface for table source.
 */
public interface TableSource extends Serializable {

    /**
     * The init method for compile time.
     */
    void init(Configuration tableConf, TableSchema tableSchema);

    /**
     * The init method for runtime.
     */
    void open(RuntimeContext context);

    /**
     * List all the partitions for the source.
     */
    List<Partition> listPartitions();

    /**
     * List all the partitions for the source.
     */
    default List<Partition> listPartitions(int parallelism) {
        return listPartitions();
    }

    /**
     * Returns the {@link TableDeserializer} for the source to convert data read from
     * the source to {@link Row}.
     */
    <IN> TableDeserializer<IN> getDeserializer(Configuration conf);

    /**
     * Fetch data for the partition from start offset.
     */
    <T> FetchData<T> fetch(Partition partition, Optional<Offset> startOffset, FetchWindow windowInfo) throws IOException;

    /**
     * The close callback for the job finish the execution.
     */
    void close();
}
