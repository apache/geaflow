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

package org.apache.geaflow.console.core.service.runtime;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.geaflow.console.common.util.HTTPUtil;
import org.apache.geaflow.console.common.util.RetryUtil;
import org.apache.geaflow.console.common.util.ZipUtil;
import org.apache.geaflow.console.common.util.ZipUtil.FileZipEntry;
import org.apache.geaflow.console.common.util.ZipUtil.GeaflowZipEntry;
import org.apache.geaflow.console.common.util.exception.GeaflowException;
import org.apache.geaflow.console.common.util.exception.GeaflowLogException;
import org.apache.geaflow.console.common.util.type.GeaflowTaskStatus;
import org.apache.geaflow.console.core.model.data.GeaflowInstance;
import org.apache.geaflow.console.core.model.file.GeaflowRemoteFile;
import org.apache.geaflow.console.core.model.job.config.GeaflowArgsClass;
import org.apache.geaflow.console.core.model.job.config.RayClientArgsClass;
import org.apache.geaflow.console.core.model.job.config.RayClusterArgsClass;
import org.apache.geaflow.console.core.model.plugin.config.RayPluginConfigClass;
import org.apache.geaflow.console.core.model.task.GeaflowTask;
import org.apache.geaflow.console.core.model.task.GeaflowTaskHandle;
import org.apache.geaflow.console.core.model.task.RayTaskHandle;
import org.apache.geaflow.console.core.model.version.GeaflowVersion;
import org.apache.geaflow.console.core.service.InstanceService;
import org.apache.geaflow.console.core.service.file.LocalFileFactory;
import org.apache.geaflow.console.core.service.file.RemoteFileStorage;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class RayRuntime implements GeaflowRuntime {


    @Autowired
    private RemoteFileStorage remoteFileStorage;

    @Getter
    @Setter
    public static class RaySubmitResponse {

        private String jobId;
        private String submissionId;

    }

    @Autowired
    private RayTaskParams taskParams;

    @Autowired
    private RemoteFileStorage fileStorage;

    @Autowired
    private LocalFileFactory localFileFactory;

    @Autowired
    private InstanceService instanceService;

    @Override
    public GeaflowTaskHandle start(GeaflowTask task) {
        GeaflowInstance instance = instanceService.get(task.getRelease().getJob().getInstanceId());
        uploadZipFiles(task);
        return doStart(task, taskParams.buildClientArgs(instance, task));
    }

    private String getSubmitJobUrl(String rayUrl) {
        return String.format("%s/api/jobs/", rayUrl);
    }

    private String getQueryJobStatusUrl(String rayUrl, String rayJobId) {
        return String.format("%s/api/jobs/%s", rayUrl, rayJobId);
    }

    private String getStopJobUrl(String rayUrl, String rayJobId) {
        return String.format("%s/api/jobs/%s/stop", rayUrl, rayJobId);
    }

    private GeaflowTaskStatus toTaskStatus(String status) {
        switch (status) {
            // pending also shown as running in console.
            case "PENDING":
            case "RUNNING":
                return GeaflowTaskStatus.RUNNING;
            case "STOPPED":
                return GeaflowTaskStatus.STOPPED;
            case "SUCCEEDED":
                return GeaflowTaskStatus.FINISHED;
            case "FAILED":
                return GeaflowTaskStatus.FAILED;
            default:
                throw new GeaflowException("Unknown status {}", status);
        }
    }

    @Override
    public GeaflowTaskStatus queryStatus(GeaflowTask task) {
        try {
            return RetryUtil.exec(() -> {
                RayPluginConfigClass rayPluginConfig = taskParams.buildRayClusterConfig(task);
                String url = getQueryJobStatusUrl(rayPluginConfig.getDashboardAddress(), task.getHandle().getAppId());
                JSONObject response = HTTPUtil.get(url);
                String status = response.get("status").toString();
                return toTaskStatus(status);
            }, 5, 500);

        } catch (Exception e) {
            log.error("Query task {} status failed, handle={}", task.getId(), JSON.toJSONString(task.getHandle()), e);
            return GeaflowTaskStatus.FAILED;
        }
    }

    @Override
    public void stop(GeaflowTask task) {
        try {
            String rayJobId = task.getHandle().getAppId();
            RayPluginConfigClass rayPluginConfig = taskParams.buildRayClusterConfig(task);
            String rayUrl = rayPluginConfig.getDashboardAddress();
            HTTPUtil.post(getStopJobUrl(rayUrl, rayJobId), new JSONObject().toJSONString());

        } catch (Exception e) {
            throw new GeaflowLogException("Stop task {} failed", task.getId(), e);
        }
    }


    private String buildRequest(GeaflowTask task, GeaflowArgsClass geaflowArgs) {
        RayClusterArgsClass clusterArgs = (RayClusterArgsClass) geaflowArgs.getClusterArgs();
        RayPluginConfigClass rayConfig = clusterArgs.getRayConfig();
        List<String> remoteJarUrls = getDownloadJarUrls(task);
        List<String> downloadJarPaths = new ArrayList<>(remoteJarUrls.size());
        for (String remoteJarUrl : remoteJarUrls) {
            String str = remoteJarUrl.replace(".zip", "");
            String result = str.replaceAll("[:/.]+", "_");
            downloadJarPaths.add(rayConfig.getSessionResourceJarPath() + result + "/*");
        }

        String downloadJarClassPath = String.join(":", downloadJarPaths);
        List<String> remoteJarUrlsStr = new ArrayList<>(remoteJarUrls.size());
        for (String remoteUrl : remoteJarUrls) {
            remoteJarUrlsStr.add("\"" + remoteUrl + "\"");
        }
        String remoteJarJsonPath = String.join(",", remoteJarUrlsStr);

        String argString = StringEscapeUtils.escapeJava(JSON.toJSONString(geaflowArgs.build()));
        argString = StringEscapeUtils.escapeJava("\"" + argString + "\"");
        return String.format("{\n" + "\"entrypoint\": \"java -classpath %s:%s -Dray.address=%s %s %s\",\n"
                + "\"runtime_env\": {\"java_jars\": [%s]}\n" + "}", rayConfig.getDistJarPath(), downloadJarClassPath,
            rayConfig.getRedisAddress(), task.getMainClass(), argString, remoteJarJsonPath);
    }

    private List<String> getDownloadJarUrls(GeaflowTask task) {
        List<String> urls = new ArrayList<>();
        GeaflowVersion version = task.getRelease().getVersion();

        String versionUrl = formatHttp(remoteFileStorage.getUrl(getVersionFilePath(version)));
        urls.add(versionUrl);
        if (CollectionUtils.isNotEmpty(task.getUserJars())) {
            String udfUrl = formatHttp(remoteFileStorage.getUrl(getTaskFilePath(task)));
            urls.add(udfUrl);
        }
        return urls;
    }

    private String formatHttp(String url) {
        return url.replace("http://", "https://");
    }

    private GeaflowTaskHandle doStart(GeaflowTask task, RayClientArgsClass clientArgs) {
        GeaflowArgsClass geaflowArgs = clientArgs.getGeaflowArgs();
        RayClusterArgsClass clusterArgs = (RayClusterArgsClass) geaflowArgs.getClusterArgs();
        try {
            String request = buildRequest(task, geaflowArgs);
            String rayUrl = clusterArgs.getRayConfig().getDashboardAddress();
            RaySubmitResponse response = HTTPUtil.post(getSubmitJobUrl(rayUrl), request, RaySubmitResponse.class);

            GeaflowTaskHandle taskHandle = new RayTaskHandle(response.submissionId);
            log.info("Start task {} success, rayUrl={}, handle={}", rayUrl, task.getId(),
                JSON.toJSONString(taskHandle));
            return taskHandle;

        } catch (Exception e) {
            throw new GeaflowLogException("Start task {} failed", task.getId(), e);
        }
    }


    private String getTaskFilePath(GeaflowTask task) {
        return RemoteFileStorage.getTaskFilePath(task.getRelease().getJob().getId(), task.getId() + "-task-udf.zip");
    }

    private String getVersionFilePath(GeaflowVersion version) {
        return RemoteFileStorage.getVersionFilePath(version.getName(), version.getEngineJarPackage().getName())
            + ".zip";
    }

    private String getVersionZipMd5Path(GeaflowVersion version, String md5) {
        return String.format("%s_%s_zip.md5",
            RemoteFileStorage.getVersionFilePath(version.getName(), version.getEngineJarPackage().getName()), md5);
    }

    private void uploadZipFiles(GeaflowTask task) {
        GeaflowVersion version = task.getRelease().getVersion();
        try {
            // check file exists.
            String zipPath = getVersionFilePath(version);
            GeaflowRemoteFile jar = version.getEngineJarPackage();
            String versionZipMd5Path = getVersionZipMd5Path(version, jar.getMd5());
            if (!remoteFileStorage.checkFileExists(versionZipMd5Path)) {
                File file = localFileFactory.getVersionFile(version.getName(), jar);
                InputStream stream = ZipUtil.buildZipInputStream(new FileZipEntry(jar.getName(), file));
                fileStorage.upload(zipPath, stream);
                // use the fileName as md5
                fileStorage.upload(versionZipMd5Path, new ByteArrayInputStream(new byte[]{}));
                log.info("upload zip file for ray {}, {}", version.getName(), versionZipMd5Path);
            }
        } catch (Exception e) {
            throw new GeaflowLogException("ZipFile engine failed {}", version.getName(), e);
        }

        List<GeaflowRemoteFile> userJars = task.getUserJars();
        List<GeaflowZipEntry> udfs = new ArrayList<>();
        try {
            for (GeaflowRemoteFile userJar : userJars) {
                File file = localFileFactory.getUserFile(userJar.getCreatorId(), userJar);
                udfs.add(new FileZipEntry(userJar.getName(), file));
            }
            if (!udfs.isEmpty()) {
                InputStream stream = ZipUtil.buildZipInputStream(udfs);
                String zipPath = getTaskFilePath(task);
                fileStorage.upload(zipPath, stream);
            }

        } catch (Exception e) {
            throw new GeaflowLogException("ZipFile udf failed", e);
        }
    }

}
