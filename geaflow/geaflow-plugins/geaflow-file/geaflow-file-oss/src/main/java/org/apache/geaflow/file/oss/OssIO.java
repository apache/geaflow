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

package org.apache.geaflow.file.oss;

import com.aliyun.oss.OSSClient;
import com.aliyun.oss.model.DeleteObjectsRequest;
import com.aliyun.oss.model.ListObjectsRequest;
import com.aliyun.oss.model.OSSObject;
import com.aliyun.oss.model.OSSObjectSummary;
import com.aliyun.oss.model.ObjectListing;
import com.aliyun.oss.model.ObjectMetadata;
import com.google.common.base.Preconditions;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.commons.io.FileUtils;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.common.utils.GsonUtil;
import org.apache.geaflow.file.FileConfigKeys;
import org.apache.geaflow.file.FileInfo;
import org.apache.geaflow.file.IPersistentIO;
import org.apache.geaflow.file.PersistentType;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

public class OssIO implements IPersistentIO {

    private OSSClient ossClient;
    private String bucketName;

    public OssIO() {

    }

    @Override
    public void init(Configuration userConfig) {
        String jsonConfig = Preconditions.checkNotNull(userConfig.getString(FileConfigKeys.JSON_CONFIG));
        Map<String, String> persistConfig = GsonUtil.parse(jsonConfig);

        this.bucketName = Configuration.getString(FileConfigKeys.OSS_BUCKET_NAME, persistConfig);
        String endpoint = Configuration.getString(FileConfigKeys.OSS_ENDPOINT, persistConfig);
        String accessKeyId = Configuration.getString(FileConfigKeys.OSS_ACCESS_ID, persistConfig);
        String accessKeySecret = Configuration.getString(FileConfigKeys.OSS_SECRET_KEY, persistConfig);
        this.ossClient = new OSSClient(endpoint, accessKeyId, accessKeySecret);
    }


    @Override
    public List<String> listFileName(Path path) throws IOException {
        FileInfo[] infos = listFileInfo(path);
        return Arrays.stream(infos).map(c -> c.getPath().getName()).collect(Collectors.toList());
    }

    @Override
    public boolean exists(Path path) throws IOException {
        boolean existFile = ossClient.doesObjectExist(bucketName, pathToKey(path));
        if (!existFile) {
            ObjectListing objectListing = ossClient.listObjects(bucketName, keyToPrefix(pathToKey(path)));
            return objectListing.getObjectSummaries().size() > 0;
        }
        return true;
    }

    @Override
    public boolean delete(Path path, boolean recursive) throws IOException {
        String key = pathToKey(path);
        boolean deleteFlag = false;

        if (recursive) {
            String nextMarker = null;
            ObjectListing objectListing;
            ListObjectsRequest request = new ListObjectsRequest(bucketName);
            request.setPrefix(keyToPrefix(key));
            do {
                request.setMarker(nextMarker);
                Preconditions.checkArgument(request.getPrefix() != null && request.getPrefix().length() > 0);
                objectListing = ossClient.listObjects(request);
                List<OSSObjectSummary> sums = objectListing.getObjectSummaries();
                List<String> files = new ArrayList<>();
                for (OSSObjectSummary s : sums) {
                    files.add(s.getKey());
                }
                nextMarker = objectListing.getNextMarker();
                if (!files.isEmpty()) {
                    ossClient.deleteObjects(new DeleteObjectsRequest(bucketName).withKeys(files));
                    deleteFlag = true;
                }
            } while (objectListing.isTruncated());
        } else {
            ossClient.deleteObject(bucketName, key);
        }

        return deleteFlag;
    }

    @Override
    public boolean renameFile(Path from, Path to) throws IOException {
        String fromKey = pathToKey(from);
        String toKey = pathToKey(to);
        String nextMarker = null;
        ObjectListing objectListing;
        ListObjectsRequest request = new ListObjectsRequest(bucketName);
        request.setPrefix(keyToPrefix(fromKey));
        do {
            request.setMarker(nextMarker);
            Preconditions.checkArgument(request.getPrefix() != null && request.getPrefix().length() > 0);
            objectListing = ossClient.listObjects(request);
            List<OSSObjectSummary> sums = objectListing.getObjectSummaries();
            for (OSSObjectSummary s : sums) {
                String key = s.getKey();
                String newKey = key.replace(fromKey, toKey);
                ossClient.copyObject(bucketName, key, bucketName, newKey);
                ossClient.deleteObject(bucketName, key);
            }
            nextMarker = objectListing.getNextMarker();
        } while (objectListing.isTruncated());

        fromKey = pathToKey(from);
        toKey = pathToKey(to);
        if (!from.toString().endsWith("/") && !to.toString().endsWith("/")
            && ossClient.doesObjectExist(bucketName, fromKey)) {
            ossClient.copyObject(bucketName, fromKey, bucketName, toKey);
            ossClient.deleteObject(bucketName, fromKey);
        }
        return true;
    }

    @Override
    public boolean createNewFile(Path path) throws IOException {
        if (exists(path)) {
            return false;
        }
        ossClient.putObject(bucketName, pathToKey(path), new ByteArrayInputStream(new byte[]{}));
        return true;
    }

    @Override
    public void copyFromLocalFile(Path local, Path remote) throws IOException {
        ossClient.putObject(bucketName, pathToKey(remote), new File(local.toString()));
    }

    @Override
    public void copyToLocalFile(Path remote, Path local) throws IOException {
        FileUtils.copyInputStreamToFile(open(remote), new File(local.toString()));
    }

    @Override
    public long getFileSize(Path path) throws IOException {
        OSSObject ossObject = ossClient.getObject(bucketName, pathToKey(path));
        return ossObject.getObjectMetadata().getContentLength();
    }

    @Override
    public long getFileCount(Path path) throws IOException {
        long count = 0;
        String nextMarker = null;
        ObjectListing objectListing;
        ListObjectsRequest request = new ListObjectsRequest(bucketName);
        request.setPrefix(keyToPrefix(pathToKey(path)));
        do {
            request.setMarker(nextMarker);
            objectListing = ossClient.listObjects(request);
            count += objectListing.getObjectSummaries().size();
            nextMarker = objectListing.getNextMarker();
        } while (objectListing.isTruncated());

        return count;
    }

    @Override
    public FileInfo getFileInfo(Path path) throws IOException {
        ObjectMetadata obj = ossClient.getObjectMetadata(bucketName, pathToKey(path));
        return FileInfo.of()
            .withPath(path)
            .withLength(obj.getContentLength())
            .withModifiedTime(obj.getLastModified().getTime());
    }

    @Override
    public FileInfo[] listFileInfo(Path path, PathFilter filter) throws IOException {
        List<FileInfo> res = Arrays.asList(listFileInfo(path));
        return res.stream().filter(c -> filter.accept(c.getPath())).toArray(FileInfo[]::new);
    }

    @Override
    public FileInfo[] listFileInfo(Path path) throws IOException {
        Set<FileInfo> res = new HashSet<>();
        String nextMarker = null;
        ObjectListing objectListing;
        ListObjectsRequest request = new ListObjectsRequest(bucketName);
        request.setPrefix(keyToPrefix(pathToKey(path)));
        int prefixLen = request.getPrefix().length();
        do {
            request.setMarker(nextMarker);
            objectListing = ossClient.listObjects(request);
            List<OSSObjectSummary> sums = objectListing.getObjectSummaries();
            for (OSSObjectSummary s : sums) {
                String str = s.getKey().substring(prefixLen);
                int nextPos = str.indexOf('/');
                Path filePath;
                long modifiedTime;
                if (nextPos == -1) {
                    filePath = new Path(keyToPath(s.getKey()));
                    modifiedTime = s.getLastModified().getTime();
                } else {
                    filePath = new Path(keyToPath(request.getPrefix() + str.substring(0, nextPos)));
                    modifiedTime = 0;
                }
                FileInfo fileInfo = FileInfo.of()
                    .withPath(filePath)
                    .withLength(s.getSize())
                    .withModifiedTime(modifiedTime);
                res.add(fileInfo);
            }
            nextMarker = objectListing.getNextMarker();
        } while (objectListing.isTruncated());
        return res.toArray(new FileInfo[0]);
    }

    @Override
    public InputStream open(Path path) throws IOException {
        OSSObject ossObject = ossClient.getObject(bucketName, pathToKey(path));
        return ossObject.getObjectContent();
    }

    @Override
    public void close() throws IOException {
        this.ossClient.shutdown();
    }

    @Override
    public PersistentType getPersistentType() {
        return PersistentType.OSS;
    }

    private String keyToPath(String key) {
        return "/" + key;
    }

    private String pathToKey(Path path) {
        String strPath = path.toUri().getPath();
        if (strPath.charAt(0) == '/') {
            return strPath.substring(1);
        }
        return strPath;
    }

    private String keyToPrefix(String key) {
        if (key.charAt(key.length() - 1) == '/') {
            return key;
        }
        return key + "/";
    }
}
