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

package org.apache.geaflow.console.core.service;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.geaflow.console.common.dal.dao.IdDao;
import org.apache.geaflow.console.common.dal.dao.ReleaseDao;
import org.apache.geaflow.console.common.dal.entity.ReleaseEntity;
import org.apache.geaflow.console.common.dal.model.ReleaseSearch;
import org.apache.geaflow.console.common.service.integration.engine.CompileContext;
import org.apache.geaflow.console.common.service.integration.engine.CompileResult;
import org.apache.geaflow.console.common.service.integration.engine.FunctionInfo;
import org.apache.geaflow.console.common.service.integration.engine.GeaflowCompiler;
import org.apache.geaflow.console.common.service.integration.engine.TableInfo;
import org.apache.geaflow.console.common.util.ListUtil;
import org.apache.geaflow.console.common.util.context.ContextHolder;
import org.apache.geaflow.console.common.util.exception.GeaflowCompileException;
import org.apache.geaflow.console.common.util.type.CatalogType;
import org.apache.geaflow.console.core.model.cluster.GeaflowCluster;
import org.apache.geaflow.console.core.model.data.GeaflowFunction;
import org.apache.geaflow.console.core.model.data.GeaflowInstance;
import org.apache.geaflow.console.core.model.file.GeaflowRemoteFile;
import org.apache.geaflow.console.core.model.job.GeaflowJob;
import org.apache.geaflow.console.core.model.job.config.CompileContextClass;
import org.apache.geaflow.console.core.model.plugin.GeaflowPlugin;
import org.apache.geaflow.console.core.model.release.GeaflowRelease;
import org.apache.geaflow.console.core.model.version.GeaflowVersion;
import org.apache.geaflow.console.core.service.config.DeployConfig;
import org.apache.geaflow.console.core.service.converter.IdConverter;
import org.apache.geaflow.console.core.service.converter.ReleaseConverter;
import org.apache.geaflow.console.core.service.file.LocalFileFactory;
import org.apache.geaflow.console.core.service.file.RemoteFileStorage;
import org.apache.geaflow.console.core.service.version.CompileClassLoader;
import org.apache.geaflow.console.core.service.version.FunctionClassLoader;
import org.apache.geaflow.console.core.service.version.VersionClassLoader;
import org.apache.geaflow.console.core.service.version.VersionFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class ReleaseService extends IdService<GeaflowRelease, ReleaseEntity, ReleaseSearch> {

    @Autowired
    private DeployConfig deployConfig;

    @Autowired
    private ReleaseDao releaseDao;

    @Autowired
    private JobService jobService;

    @Autowired
    private ReleaseConverter releaseConverter;

    @Autowired
    private VersionService versionService;

    @Autowired
    private InstanceService instanceService;

    @Autowired
    private ClusterService clusterService;

    @Autowired
    private VersionFactory versionFactory;

    @Autowired
    private RemoteFileStorage remoteFileStorage;

    @Autowired
    private LocalFileFactory localFileFactory;

    protected IdDao<ReleaseEntity, ReleaseSearch> getDao() {
        return releaseDao;
    }

    @Override
    protected IdConverter<GeaflowRelease, ReleaseEntity> getConverter() {
        return releaseConverter;
    }

    @Override
    protected List<GeaflowRelease> parse(List<ReleaseEntity> releaseEntities) {
        return releaseEntities.stream().map(e -> {
            GeaflowJob job = jobService.get(e.getJobId());

            GeaflowVersion version = versionService.get(e.getVersionId());

            GeaflowCluster cluster = clusterService.get(e.getClusterId());

            return releaseConverter.convert(e, job, version, cluster);
        }).collect(Collectors.toList());
    }

    public Set<FunctionInfo> parseFunctions(GeaflowJob job, GeaflowVersion version) {
        CompilerAndContext compilerAndContext = getCompilerAndContext(version, job.getInstanceId(), CatalogType.MEMORY);
        try {
            return compilerAndContext.getCompiler()
                .getUnResolvedFunctions(job.getUserCode().getText(), compilerAndContext.getContext());
        } catch (Exception e) {
            throw new GeaflowCompileException("Parse functions failed", e);
        }
    }


    public CompileResult compile(GeaflowJob job, GeaflowVersion version, Map<String, Integer> parallelisms) {
        VersionClassLoader classLoader = versionFactory.getClassLoader(version);
        List<GeaflowRemoteFile> udfs = ListUtil.convert(job.getFunctions(), GeaflowFunction::getJarPackage);
        List<GeaflowRemoteFile> plugins = ListUtil.convert(job.getPlugins(), GeaflowPlugin::getJarPackage);
        udfs.addAll(plugins);
        if (CollectionUtils.isNotEmpty(udfs)) {
            // use FunctionClassLoader if job has udf
            FunctionClassLoader functionLoader = null;
            try {
                functionLoader = new FunctionClassLoader(classLoader, udfs);
                return compile(functionLoader, job, parallelisms);
            } finally {
                if (functionLoader != null) {
                    functionLoader.closeClassLoader();
                }

            }
        }

        return compile(classLoader, job, parallelisms);
    }


    private CompileResult compile(CompileClassLoader classLoader, GeaflowJob job, Map<String, Integer> parallelisms) {
        CompilerAndContext compilerAndContext = getCompilerAndContext(classLoader, job.getInstanceId(), CatalogType.CONSOLE);
        if (MapUtils.isNotEmpty(parallelisms)) {
            compilerAndContext.getContext().setParallelisms(parallelisms);
        }
        try {
            return compilerAndContext.getCompiler()
                .compile(job.getUserCode().getText(), compilerAndContext.getContext());
        } catch (Exception e) {
            throw new GeaflowCompileException("Compile job code failed", e);
        }
    }

    public void dropByJobIds(List<String> ids) {
        List<ReleaseEntity> releases = releaseDao.getByJobIds(ids);
        for (ReleaseEntity release : releases) {
            for (int i = 1; i <= release.getVersion(); i++) {
                String path = RemoteFileStorage.getPackageFilePath(release.getJobId(), i);
                remoteFileStorage.delete(path);
            }
        }
        releaseDao.dropByJobIds(ids);
    }

    public Set<String> parseDeclaredPlugins(GeaflowJob job, GeaflowVersion version) {
        CompilerAndContext compilerAndContext = getCompilerAndContext(version, job.getInstanceId(), CatalogType.MEMORY);
        try {
            return compilerAndContext.getCompiler()
                .getDeclaredTablePlugins(job.getUserCode().getText(), compilerAndContext.getContext());
        } catch (Exception e) {
            throw new GeaflowCompileException("Parse plugins failed", e);
        }
    }

    public Set<TableInfo> getUnResolvedTables(GeaflowJob job, GeaflowVersion version) {
        CompilerAndContext compilerAndContext = getCompilerAndContext(version, job.getInstanceId(), CatalogType.MEMORY);
        try {
            return compilerAndContext.getCompiler()
                .getUnResolvedTables(job.getUserCode().getText(), compilerAndContext.getContext());
        } catch (Exception e) {
            throw new GeaflowCompileException("Parse plugins failed", e);
        }
    }

    private CompilerAndContext getCompilerAndContext(GeaflowVersion version, String instanceId, CatalogType catalogType) {
        VersionClassLoader classLoader = versionFactory.getClassLoader(version);
        return getCompilerAndContext(classLoader, instanceId, catalogType);
    }

    private CompilerAndContext getCompilerAndContext(CompileClassLoader classLoader, String instanceId, CatalogType catalogType) {
        CompileContext context = classLoader.newInstance(CompileContext.class);
        GeaflowCompiler compiler = classLoader.newInstance(GeaflowCompiler.class);
        setContextConfig(context, instanceId, catalogType);
        return new CompilerAndContext(compiler, context);
    }

    private void setContextConfig(CompileContext context, String instanceId, CatalogType catalogType) {
        GeaflowInstance instance = instanceService.get(instanceId);
        CompileContextClass config = new CompileContextClass();
        config.setTokenKey(ContextHolder.get().getSessionToken());
        config.setInstanceName(instance.getName());
        config.setCatalogType(catalogType.getValue());
        config.setEndpoint(deployConfig.getGatewayUrl());
        Map<String, String> map = config.build().toStringMap();
        context.setConfig(map);
    }

    private static class CompilerAndContext {

        private final GeaflowCompiler compiler;
        private final CompileContext context;

        public CompilerAndContext(GeaflowCompiler geaflowCompiler, CompileContext compileContext) {
            this.compiler = geaflowCompiler;
            this.context = compileContext;
        }

        public GeaflowCompiler getCompiler() {
            return compiler;
        }

        public CompileContext getContext() {
            return context;
        }
    }

}

