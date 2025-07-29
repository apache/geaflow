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

import com.google.common.base.Preconditions;
import java.io.InputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.annotation.PostConstruct;
import javax.servlet.http.HttpServletResponse;
import org.apache.commons.lang3.StringUtils;
import org.apache.geaflow.console.biz.shared.RemoteFileManager;
import org.apache.geaflow.console.biz.shared.convert.NameViewConverter;
import org.apache.geaflow.console.biz.shared.convert.RemoteFileViewConverter;
import org.apache.geaflow.console.biz.shared.view.RemoteFileView;
import org.apache.geaflow.console.common.dal.entity.RemoteFileEntity;
import org.apache.geaflow.console.common.dal.model.RemoteFileSearch;
import org.apache.geaflow.console.common.util.HTTPUtil;
import org.apache.geaflow.console.common.util.ListUtil;
import org.apache.geaflow.console.common.util.Md5Util;
import org.apache.geaflow.console.common.util.exception.GeaflowException;
import org.apache.geaflow.console.common.util.type.GeaflowFileType;
import org.apache.geaflow.console.common.util.type.GeaflowResourceType;
import org.apache.geaflow.console.core.model.file.GeaflowRemoteFile;
import org.apache.geaflow.console.core.service.FunctionService;
import org.apache.geaflow.console.core.service.JobService;
import org.apache.geaflow.console.core.service.NameService;
import org.apache.geaflow.console.core.service.PluginService;
import org.apache.geaflow.console.core.service.RemoteFileService;
import org.apache.geaflow.console.core.service.VersionService;
import org.apache.geaflow.console.core.service.file.FileRefService;
import org.apache.geaflow.console.core.service.file.RemoteFileStorage;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.multipart.MultipartFile;

@Service
public class RemoteFileManagerImpl extends
    NameManagerImpl<GeaflowRemoteFile, RemoteFileView, RemoteFileSearch> implements RemoteFileManager {

    @Autowired
    private RemoteFileViewConverter remoteFileViewConverter;

    @Autowired
    private RemoteFileService remoteFileService;

    @Autowired
    private RemoteFileStorage remoteFileStorage;

    @Autowired
    private FunctionService functionService;

    @Autowired
    private VersionService versionService;

    @Autowired
    private JobService jobService;

    @Autowired
    private PluginService pluginService;

    private final Map<GeaflowResourceType, FileRefService> fileServiceMap = new HashMap<>();

    @PostConstruct
    public void init() {
        fileServiceMap.put(GeaflowResourceType.JOB, jobService);
        fileServiceMap.put(GeaflowResourceType.ENGINE_VERSION, versionService);
        fileServiceMap.put(GeaflowResourceType.FUNCTION, functionService);
        fileServiceMap.put(GeaflowResourceType.PLUGIN, pluginService);
    }

    @Override
    protected NameViewConverter<GeaflowRemoteFile, RemoteFileView> getConverter() {
        return remoteFileViewConverter;
    }

    @Override
    protected NameService<GeaflowRemoteFile, RemoteFileEntity, RemoteFileSearch> getService() {
        return remoteFileService;
    }

    @Override
    protected List<GeaflowRemoteFile> parse(List<RemoteFileView> views) {
        return ListUtil.convert(views, v -> remoteFileViewConverter.convert(v));
    }

    private boolean checkFileName(String str) {
        String pattern = "^[A-Za-z0-9@!#$%^&*()_+\\-=\\[\\]{};:'\"\\,\\.\\/\\?\\\\|`~\\s]+$";
        Pattern regex = Pattern.compile(pattern);
        Matcher matcher = regex.matcher(str);
        return matcher.matches();
    }

    @Transactional
    @Override
    public String create(RemoteFileView view, MultipartFile multipartFile) {
        String name = Optional.ofNullable(view.getName()).orElse(multipartFile.getOriginalFilename());
        if (!checkFileName(name)) {
            throw new GeaflowException("File name is illegal, {}", name);
        }
        String path = Preconditions.checkNotNull(view.getPath(), "Invalid path");
        String remoteFileId;
        try {
            view.setName(name);
            view.setType(GeaflowFileType.of(StringUtils.substringAfterLast(multipartFile.getOriginalFilename(), ".")));
            view.setMd5(Md5Util.encodeFile(multipartFile));
            // set url according to the path
            String url = remoteFileStorage.getUrl(view.getPath());
            view.setUrl(url);
            remoteFileId = create(view);
            view.setId(remoteFileId);
        } catch (Exception e) {
            throw new GeaflowException("Create file {} failed", name, e);
        }

        try (InputStream inputStream = multipartFile.getInputStream()) {
            remoteFileStorage.upload(path, inputStream);

        } catch (Exception e) {
            throw new GeaflowException("Upload file {} failed", name, path, e);
        }

        return remoteFileId;
    }

    @Transactional
    @Override
    public boolean upload(String remoteFileId, MultipartFile multipartFile) {
        remoteFileService.validateUpdateIds(Collections.singletonList(remoteFileId));
        GeaflowRemoteFile remoteFile = remoteFileService.get(remoteFileId);
        if (remoteFile == null) {
            return false;
        }

        String name = remoteFile.getName();
        String path = remoteFile.getPath();

        try {
            String md5 = Md5Util.encodeFile(multipartFile);
            if (md5.equals(remoteFile.getMd5())) {
                return false;
            }
            remoteFileService.updateMd5ById(remoteFileId, md5);
            remoteFileService.updateUrlById(remoteFileId, remoteFileStorage.getUrl(path));
        } catch (Exception e) {
            throw new GeaflowException("Update file {} md5 failed", name, e);
        }

        try (InputStream inputStream = multipartFile.getInputStream()) {
            remoteFileStorage.upload(path, inputStream);

        } catch (Exception e) {
            throw new GeaflowException("Upload file {} failed", name, path, e);
        }

        return true;
    }

    @Override
    public boolean download(String remoteFileId, HttpServletResponse response) {
        remoteFileService.validateGetIds(Collections.singletonList(remoteFileId));
        GeaflowRemoteFile remoteFile = remoteFileService.get(remoteFileId);
        if (remoteFile == null) {
            throw new GeaflowException("File not found");
        }

        String path = remoteFile.getPath();
        String name = remoteFile.getName();

        try (InputStream input = remoteFileStorage.download(path)) {
            HTTPUtil.download(response, input, name);

        } catch (Exception e) {
            throw new GeaflowException("Download file {} from {} failed", name, path, e);
        }

        return true;
    }

    @Override
    public boolean delete(String remoteFileId) {
        remoteFileService.validateUpdateIds(Collections.singletonList(remoteFileId));
        // throw exception if file is used by others
        checkFileUsed(remoteFileId);
        return deleteFile(remoteFileId);
    }

    @Override
    public List<RemoteFileView> get(List<String> ids) {
        remoteFileService.validateGetIds(ids);
        return super.get(ids);
    }


    @Override
    public void deleteRefJar(String jarId, String refId, GeaflowResourceType resourceType) {
        if (jarId == null || !fileServiceMap.containsKey(resourceType)) {
            return;
        }

        // version jar is only used by a version, do not filter
        if (resourceType != GeaflowResourceType.ENGINE_VERSION) {
            for (Entry<GeaflowResourceType, FileRefService> entry : fileServiceMap.entrySet()) {
                FileRefService fileRefService = entry.getValue();
                // exclude itself when current type is the resourceType
                long refCount = entry.getKey() == resourceType ? fileRefService.getFileRefCount(jarId, refId) :
                    fileRefService.getFileRefCount(jarId, null);
                if (refCount > 0) {
                    return;
                }
            }
        }

        deleteFile(jarId);
    }

    private void checkFileUsed(String remoteFileId) {
        for (Entry<GeaflowResourceType, FileRefService> entry : fileServiceMap.entrySet()) {
            long functionCount = entry.getValue().getFileRefCount(remoteFileId, null);
            if (functionCount > 0) {
                throw new GeaflowException("file {} is used by {}, count:{}",
                    remoteFileId, entry.getKey().name(), functionCount);
            }
        }
    }

    private boolean deleteFile(String remoteFileId) {
        GeaflowRemoteFile remoteFile = remoteFileService.get(remoteFileId);
        if (remoteFile == null) {
            return false;
        }

        String id = remoteFile.getId();
        String name = remoteFile.getName();
        String path = remoteFile.getPath();

        try {
            remoteFileStorage.delete(path);
            drop(id);

        } catch (Exception e) {
            throw new GeaflowException("Delete file {} failed", name, e);
        }

        return true;
    }

}
