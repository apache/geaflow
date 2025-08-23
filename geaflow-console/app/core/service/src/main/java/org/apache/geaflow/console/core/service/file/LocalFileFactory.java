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

package org.apache.geaflow.console.core.service.file;

import java.io.File;
import java.io.InputStream;
import lombok.extern.slf4j.Slf4j;
import org.apache.geaflow.console.common.util.FileUtil;
import org.apache.geaflow.console.common.util.Fmt;
import org.apache.geaflow.console.common.util.Md5Util;
import org.apache.geaflow.console.core.model.file.GeaflowRemoteFile;
import org.apache.geaflow.console.core.model.release.GeaflowRelease;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class LocalFileFactory {

    public static final String LOCAL_VERSION_FILE_DIRECTORY = "/tmp/geaflow/local/versions";

    public static final String LOCAL_TASK_FILE_DIRECTORY = "/tmp/geaflow/local/tasks";

    public static final String LOCAL_USER_FILE_DIRECTORY = "/tmp/geaflow/local/users";

    @Autowired
    private RemoteFileStorage remoteFileStorage;

    public File getVersionFile(String versionName, GeaflowRemoteFile remoteFile) {
        String filePath = getVersionFilePath(versionName, remoteFile.getName());
        return downloadFileWithMd5(remoteFile.getPath(), filePath, remoteFile.getMd5());
    }

    public File getUserFile(String userId, GeaflowRemoteFile remoteFile) {
        String filePath = getUserFilePath(userId, remoteFile.getName());
        return downloadFileWithMd5(remoteFile.getPath(), filePath, remoteFile.getMd5());
    }

    public File getTaskUserFile(String runtimeTaskId, GeaflowRemoteFile remoteFile) {
        String filePath = getTaskFilePath(runtimeTaskId, remoteFile.getName());
        return downloadFileWithMd5(remoteFile.getPath(), filePath, remoteFile.getMd5());
    }

    public File getTaskReleaseFile(String runtimeTaskId, String jobId, GeaflowRelease release) {
        String path = RemoteFileStorage.getPackageFilePath(jobId, release.getReleaseVersion());
        String filePath = getTaskFilePath(runtimeTaskId, new File(path).getName());
        return downloadFileWithMd5(path, filePath, release.getMd5());
    }

    public static String getVersionFilePath(String versionName, String fileName) {
        return Fmt.as("{}/{}/{}", LOCAL_VERSION_FILE_DIRECTORY, versionName, fileName);
    }

    public static String getTaskFilePath(String runtimeTaskId, String fileName) {
        return Fmt.as("{}/{}/{}", LOCAL_TASK_FILE_DIRECTORY, runtimeTaskId, fileName);
    }

    public static String getUserFilePath(String userId, String fileName) {
        return Fmt.as("{}/{}/{}", LOCAL_USER_FILE_DIRECTORY, userId, fileName);
    }

    private File downloadFileWithMd5(String remotePath, String localPath, String md5) {
        // check file md5
        if (!md5.equals(loadFileMd5(localPath))) {
            // delete local files
            FileUtil.delete(getMd5FilePath(localPath));
            FileUtil.delete(localPath);

            // download file
            downloadFile(remotePath, localPath);

            // save file md5
            FileUtil.writeFile(getMd5FilePath(localPath), Md5Util.encodeFile(localPath));
        }

        return new File(localPath);
    }

    private String getMd5FilePath(String filePath) {
        return filePath + ".md5";
    }

    private String loadFileMd5(String filePath) {
        if (!FileUtil.exist(filePath)) {
            return null;
        }

        String md5FilePath = getMd5FilePath(filePath);
        if (!FileUtil.exist(md5FilePath)) {
            return null;
        }

        return FileUtil.readFileContent(md5FilePath).trim();
    }

    private void downloadFile(String remotePath, String localPath) {
        InputStream stream = remoteFileStorage.download(remotePath);
        FileUtil.writeFile(localPath, stream);
        log.info("Download file {} from {} success", localPath, remotePath);
    }
}
