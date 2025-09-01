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

package org.apache.geaflow.console.biz.shared.impl;

import static org.apache.geaflow.console.core.service.RemoteFileService.JAR_FILE_SUFFIX;

import com.google.common.base.Preconditions;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.geaflow.console.biz.shared.RemoteFileManager;
import org.apache.geaflow.console.biz.shared.VersionManager;
import org.apache.geaflow.console.biz.shared.convert.NameViewConverter;
import org.apache.geaflow.console.biz.shared.convert.VersionViewConverter;
import org.apache.geaflow.console.biz.shared.view.IdView;
import org.apache.geaflow.console.biz.shared.view.RemoteFileView;
import org.apache.geaflow.console.biz.shared.view.VersionView;
import org.apache.geaflow.console.common.dal.model.PageList;
import org.apache.geaflow.console.common.dal.model.VersionSearch;
import org.apache.geaflow.console.common.util.FileUtil;
import org.apache.geaflow.console.common.util.I18nUtil;
import org.apache.geaflow.console.common.util.context.ContextHolder;
import org.apache.geaflow.console.common.util.exception.GeaflowException;
import org.apache.geaflow.console.common.util.exception.GeaflowIllegalException;
import org.apache.geaflow.console.common.util.type.GeaflowResourceType;
import org.apache.geaflow.console.core.model.file.GeaflowRemoteFile;
import org.apache.geaflow.console.core.model.version.GeaflowVersion;
import org.apache.geaflow.console.core.service.NameService;
import org.apache.geaflow.console.core.service.RemoteFileService;
import org.apache.geaflow.console.core.service.VersionService;
import org.apache.geaflow.console.core.service.file.LocalFileFactory;
import org.apache.geaflow.console.core.service.file.RemoteFileStorage;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.multipart.MultipartFile;

@Service
@Slf4j
public class VersionManagerImpl extends NameManagerImpl<GeaflowVersion, VersionView, VersionSearch> implements
    VersionManager {

    private static final String ENGINE_JAR_PREFIX = "";

    private static final String LANG_JAR_PREFIX = "lang-";

    private static final String GEAFLOW_DEFAULT_VERSION_NAME = "defaultVersion";

    @Autowired
    private VersionService versionService;

    @Autowired
    private VersionViewConverter versionViewConverter;

    @Autowired
    private RemoteFileService remoteFileService;

    @Autowired
    private RemoteFileManager remoteFileManager;

    @Autowired
    private RemoteFileStorage remoteFileStorage;

    @Override
    protected NameViewConverter<GeaflowVersion, VersionView> getConverter() {
        return versionViewConverter;
    }

    @Override
    protected List<GeaflowVersion> parse(List<VersionView> views) {
        return views.stream().map(e -> {
            GeaflowRemoteFile engineJar = remoteFileService.get(
                Optional.ofNullable(e.getEngineJarPackage()).map(IdView::getId).orElse(null));
            GeaflowRemoteFile langJar = remoteFileService.get(
                Optional.ofNullable(e.getLangJarPackage()).map(IdView::getId).orElse(null));
            return versionViewConverter.convert(e, engineJar, langJar);
        }).collect(Collectors.toList());
    }

    @Override
    protected NameService<GeaflowVersion, ?, VersionSearch> getService() {
        return versionService;
    }

    public PageList<VersionView> searchVersions(VersionSearch search) {
        // only system admin can see none published version
        if (!ContextHolder.get().isSystemSession()) {
            search.setPublish(true);
        }
        return super.search(search);
    }

    @Override
    public VersionView getVersion(String name) {
        // only system admin can see none published version
        if (ContextHolder.get().isSystemSession()) {
            return getByName(name);
        }

        return build(versionService.getPublishVersionByName(name));
    }

    @Override
    public String createDefaultVersion() {
        // in case of remote file config changed
        remoteFileStorage.reset();

        String path = LocalFileFactory.getVersionFilePath(GEAFLOW_DEFAULT_VERSION_NAME,
            GEAFLOW_DEFAULT_VERSION_NAME + ".jar");
        if (!FileUtil.exist(path)) {
            throw new GeaflowIllegalException("No geaflow jar found in {}", path);
        }

        VersionView versionView = new VersionView();
        versionView.setName(GEAFLOW_DEFAULT_VERSION_NAME);
        versionView.setComment(I18nUtil.getMessage("i18n.key.default.version"));
        versionView.setPublish(true);

        return createVersion(versionView, new LocalMultipartFile(new File(path)), null);
    }

    @Transactional
    @Override
    public String createVersion(VersionView versionView, MultipartFile engineJarFile, MultipartFile langJarFile) {
        String versionName = versionView.getName();
        if (StringUtils.isBlank(versionName)) {
            throw new GeaflowIllegalException("Invalid version name");
        }

        if (versionService.existName(versionName)) {
            throw new GeaflowIllegalException("Version name {} exists", versionName);
        }

        Preconditions.checkNotNull(engineJarFile, "Invalid engineJarfile");
        versionView.setEngineJarPackage(createRemoteFile(versionName, engineJarFile, ENGINE_JAR_PREFIX));

        if (langJarFile != null) {
            versionView.setLangJarPackage(createRemoteFile(versionName, langJarFile, LANG_JAR_PREFIX));
        }

        return super.create(versionView);
    }

    @Transactional
    @Override
    public boolean updateVersion(String name, VersionView updateView, MultipartFile engineJarFile,
                                 MultipartFile langJarFile) {
        VersionView view = getByName(name);
        if (view == null) {
            throw new GeaflowIllegalException("Version name {} not exists", name);
        }

        if (engineJarFile != null) {
            updateView.setEngineJarPackage(updateEngineJarFile(view, engineJarFile));
        }

        if (langJarFile != null) {
            updateView.setLangJarPackage(updateLangJarFile(view, langJarFile));
        }

        return updateById(view.getId(), updateView);
    }

    @Transactional
    @Override
    public boolean deleteVersion(String versionName) {
        GeaflowVersion version = versionService.getByName(versionName);
        if (version == null) {
            return false;
        }

        GeaflowRemoteFile engineJarPackage = version.getEngineJarPackage();
        if (engineJarPackage != null) {
            remoteFileManager.deleteRefJar(engineJarPackage.getId(), null, GeaflowResourceType.ENGINE_VERSION);
        }

        GeaflowRemoteFile langJarPackage = version.getLangJarPackage();
        if (langJarPackage != null) {
            remoteFileManager.deleteRefJar(langJarPackage.getId(), null, GeaflowResourceType.ENGINE_VERSION);
        }

        return drop(version.getId());
    }


    private RemoteFileView createRemoteFile(String versionName, MultipartFile multipartFile, String filePrefix) {
        if (!StringUtils.endsWith(multipartFile.getOriginalFilename(), JAR_FILE_SUFFIX)) {
            throw new GeaflowIllegalException("Invalid jar file");
        }

        String fileName = filePrefix + versionName + JAR_FILE_SUFFIX;
        String path = RemoteFileStorage.getVersionFilePath(versionName, fileName);

        RemoteFileView remoteFileView = new RemoteFileView();
        remoteFileView.setName(fileName);
        remoteFileView.setPath(path);
        remoteFileManager.create(remoteFileView, multipartFile);

        return remoteFileView;
    }

    private RemoteFileView updateEngineJarFile(VersionView versionView, MultipartFile multipartFile) {
        if (!StringUtils.endsWith(multipartFile.getOriginalFilename(), JAR_FILE_SUFFIX)) {
            throw new GeaflowIllegalException("Invalid jar file");
        }

        RemoteFileView engineJarPackage = versionView.getEngineJarPackage();
        if (engineJarPackage == null) {
            return createRemoteFile(versionView.getName(), multipartFile, ENGINE_JAR_PREFIX);

        } else {
            String remoteFileId = engineJarPackage.getId();
            remoteFileManager.upload(remoteFileId, multipartFile);
            return null;
        }
    }

    private RemoteFileView updateLangJarFile(VersionView versionView, MultipartFile multipartFile) {
        if (!StringUtils.endsWith(multipartFile.getOriginalFilename(), JAR_FILE_SUFFIX)) {
            throw new GeaflowIllegalException("Invalid jar file");
        }

        RemoteFileView langJarPackage = versionView.getLangJarPackage();
        if (langJarPackage == null) {
            return createRemoteFile(versionView.getName(), multipartFile, LANG_JAR_PREFIX);

        } else {
            String remoteFileId = langJarPackage.getId();
            remoteFileManager.upload(remoteFileId, multipartFile);
            return null;
        }
    }

    @AllArgsConstructor
    private static class LocalMultipartFile implements MultipartFile {

        private final File file;

        @Override
        public String getName() {
            return file.getName();
        }

        @Override
        public String getOriginalFilename() {
            return file.getName();
        }

        @Override
        public String getContentType() {
            throw new GeaflowException("Not supported");
        }

        @Override
        public boolean isEmpty() {
            throw new GeaflowException("Not supported");
        }

        @Override
        public long getSize() {
            throw new GeaflowException("Not supported");
        }

        @Override
        public byte[] getBytes() throws IOException {
            throw new GeaflowException("Not supported");
        }

        @Override
        public InputStream getInputStream() throws IOException {
            return FileUtils.openInputStream(file);
        }

        @Override
        public void transferTo(File dest) throws IOException, IllegalStateException {
            throw new GeaflowException("Not supported");
        }
    }
}
