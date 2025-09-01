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

package org.apache.geaflow.dsl.connector.hudi;

import java.util.Collections;
import java.util.List;
import org.apache.geaflow.api.context.RuntimeContext;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.common.config.keys.ConnectorConfigKeys;
import org.apache.geaflow.dsl.connector.file.FileConnectorUtil;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hudi.BaseHoodieTableFileIndex;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.HoodieTableQueryType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.Option;

public class GeaFlowHoodieTableFileIndex extends BaseHoodieTableFileIndex {


    private GeaFlowHoodieTableFileIndex(HoodieEngineContext engineContext,
                                        HoodieTableMetaClient metaClient,
                                        TypedProperties configProperties,
                                        HoodieTableQueryType queryType,
                                        List<Path> queryPaths,
                                        Option<String> specifiedQueryInstant,
                                        boolean shouldIncludePendingCommits, boolean shouldValidateInstant,
                                        FileStatusCache fileStatusCache, boolean shouldListLazily) {
        super(engineContext, metaClient, configProperties, queryType, queryPaths, specifiedQueryInstant,
            shouldIncludePendingCommits, shouldValidateInstant, fileStatusCache, shouldListLazily);
    }

    public static GeaFlowHoodieTableFileIndex create(RuntimeContext context, Configuration tableConf) {
        HoodieEngineContext engineContext = GeaFlowEngineContext.create(context, tableConf);
        String path = tableConf.getString(ConnectorConfigKeys.GEAFLOW_DSL_FILE_PATH);

        HoodieTableMetaClient metaClient = HoodieTableMetaClient.builder()
            .setMetaserverConfig(tableConf.getConfigMap())
            .setBasePath(path)
            .setConf(FileConnectorUtil.toHadoopConf(tableConf))
            .build();
        TypedProperties configProperties = HoodieUtil.toTypeProperties(tableConf.getConfigMap());

        FileStatusCache noCache = new FileStatusCache() {
            @Override
            public Option<FileStatus[]> get(Path path) {
                return Option.empty();
            }

            @Override
            public void put(Path path, FileStatus[] fileStatuses) {

            }

            @Override
            public void invalidate() {

            }
        };

        return new GeaFlowHoodieTableFileIndex(
            engineContext, metaClient,
            configProperties, HoodieTableQueryType.SNAPSHOT,
            Collections.singletonList(new Path(path)),
            Option.empty(), false, false,
            noCache, false);
    }

    @Override
    protected Object[] doParsePartitionColumnValues(String[] strings, String s) {
        return new Object[0];
    }

    @Override
    public List<PartitionPath> getAllQueryPartitionPaths() {
        return super.getAllQueryPartitionPaths();
    }
}
