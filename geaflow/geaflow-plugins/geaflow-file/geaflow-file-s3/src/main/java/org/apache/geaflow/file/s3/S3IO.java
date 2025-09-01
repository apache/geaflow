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

package org.apache.geaflow.file.s3;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.common.exception.GeaflowRuntimeException;
import org.apache.geaflow.common.utils.GsonUtil;
import org.apache.geaflow.file.FileConfigKeys;
import org.apache.geaflow.file.FileInfo;
import org.apache.geaflow.file.IPersistentIO;
import org.apache.geaflow.file.PersistentType;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.ChecksumMode;
import software.amazon.awssdk.services.s3.model.CommonPrefix;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectResponse;
import software.amazon.awssdk.services.s3.model.HeadBucketRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.NoSuchBucketException;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;
import software.amazon.awssdk.services.s3.model.S3Object;
import software.amazon.awssdk.services.s3.multipart.MultipartConfiguration;
import software.amazon.awssdk.transfer.s3.S3TransferManager;
import software.amazon.awssdk.transfer.s3.model.DownloadFileRequest;
import software.amazon.awssdk.transfer.s3.model.FileDownload;
import software.amazon.awssdk.transfer.s3.model.FileUpload;
import software.amazon.awssdk.transfer.s3.model.UploadFileRequest;

public class S3IO implements IPersistentIO {

    private static final Logger LOGGER = LoggerFactory.getLogger(S3IO.class);

    public static final int S3_MAX_RETURN_KEYS = 1000;
    private static final String PATH_DELIMITER = "/";
    private static final String LOCAL_RENAME_DELIMITER = "-";
    private static final String LOCAL_RENAME_DIR = "/tmp";

    private S3AsyncClient s3Client;
    private S3TransferManager transferManager;
    private String bucketName;
    private int inputStreamChunkSize;


    public static String filePathToKey(String bucketName, Path dirPath) {
        String pathStr = dirPath.toString();
        pathStr = pathStr.startsWith(bucketName) ? pathStr.substring(bucketName.length()) : pathStr;
        pathStr = pathStr.startsWith(PATH_DELIMITER) ? pathStr.substring(1) : pathStr;

        return pathStr;
    }

    public static String dirPathToPrefix(String bucketName, Path dirPath) {
        String pathStr = dirPath.toString();
        pathStr = pathStr.startsWith(bucketName) ? pathStr.substring(bucketName.length()) : pathStr;
        pathStr = pathStr.startsWith(PATH_DELIMITER) ? pathStr.substring(1) : pathStr;
        pathStr = pathStr.endsWith(PATH_DELIMITER) ? pathStr : pathStr + PATH_DELIMITER;

        return PATH_DELIMITER.equals(pathStr) ? "" : pathStr;
    }

    public static Path keyToFilePath(String bucketName, String key) {
        return new Path(bucketName + PATH_DELIMITER + key);
    }

    @Override
    public void init(Configuration userConfig) {
        String jsonConfig = Preconditions.checkNotNull(
            userConfig.getString(FileConfigKeys.JSON_CONFIG));
        Map<String, String> persistConfig = GsonUtil.parse(jsonConfig);

        this.bucketName = Configuration.getString(FileConfigKeys.S3_BUCKET_NAME, persistConfig);
        String endpoint = Configuration.getString(FileConfigKeys.S3_ENDPOINT, persistConfig);
        String accessKeyId = Configuration.getString(FileConfigKeys.S3_ACCESS_KEY_ID,
            persistConfig);
        String accessKey = Configuration.getString(FileConfigKeys.S3_ACCESS_KEY, persistConfig);
        long minimumPartSizeInBytes = Configuration.getLong(FileConfigKeys.S3_MIN_PART_SIZE,
            persistConfig);
        this.inputStreamChunkSize = Configuration.getInteger(
            FileConfigKeys.S3_INPUT_STREAM_CHUNK_SIZE, persistConfig);

        AwsBasicCredentials awsCreds = AwsBasicCredentials.create(accessKeyId, accessKey);

        this.s3Client = S3AsyncClient.builder().endpointOverride(URI.create(endpoint))
            .region(Region.of(Configuration.getString(FileConfigKeys.S3_REGION, persistConfig)))
            .credentialsProvider(StaticCredentialsProvider.create(awsCreds)).forcePathStyle(true)
            .multipartConfiguration(
                MultipartConfiguration.builder().minimumPartSizeInBytes(minimumPartSizeInBytes)
                    .apiCallBufferSizeInBytes(minimumPartSizeInBytes).build())
            .serviceConfiguration(builder -> builder.checksumValidationEnabled(false)).build();
        this.transferManager = S3TransferManager.builder().s3Client(s3Client).build();

        checkAndCreateBucket();
    }

    public void checkAndCreateBucket() {
        if (bucketNotExists()) {
            CreateBucketRequest createBucketRequest = CreateBucketRequest.builder()
                .bucket(bucketName).build();
            try {
                s3Client.createBucket(createBucketRequest);
                LOGGER.info("create new bucket success, bucket name: {}", bucketName);
            } catch (Throwable e) {
                throw new GeaflowRuntimeException("Failed to create bucket: " + bucketName, e);
            }
        }
    }

    private boolean bucketNotExists() {
        HeadBucketRequest headBucketRequest = HeadBucketRequest.builder().bucket(bucketName)
            .build();
        CompletableFuture<Boolean> future = s3Client.headBucket(headBucketRequest)
            .handle((response, error) -> {
                if (error != null) {
                    Throwable cause = error.getCause();
                    if (cause instanceof NoSuchBucketException) {
                        return true;
                    }

                    throw new GeaflowRuntimeException(error);
                }
                LOGGER.info("bucket already exists, bucket name: {}", bucketName);
                return false;
            });

        try {
            return future.get();
        } catch (InterruptedException | ExecutionException e) {
            throw new GeaflowRuntimeException(
                "Error checking if bucket exists, bucket name: " + bucketName, e);
        }
    }

    @Override
    public List<String> listFileName(Path path) throws IOException {
        FileInfo[] fileInfos = listFileInfo(path);
        return Arrays.stream(fileInfos).map(c -> c.getPath().getName())
            .collect(Collectors.toList());
    }

    @Override
    public boolean exists(Path path) throws IOException {
        return isFile(path) || isDirectory(path);
    }

    public boolean isFile(Path path) throws IOException {
        String key = filePathToKey(bucketName, path);

        if (key.isEmpty()) {
            return false;
        }

        HeadObjectRequest request = HeadObjectRequest.builder().bucket(bucketName).key(key).build();

        CompletableFuture<Boolean> future = s3Client.headObject(request)
            .handle((response, error) -> {
                if (error != null) {
                    if (error.getCause() instanceof NoSuchKeyException) {
                        return false;
                    }
                    throw new GeaflowRuntimeException(error);
                }
                return true;
            });

        try {
            return future.get();
        } catch (Throwable e) {
            throw new IOException("Occur error in isDirectory, path: " + path, e);
        }
    }

    public boolean isDirectory(Path path) throws IOException {
        String prefix = dirPathToPrefix(bucketName, path);
        String key = filePathToKey(bucketName, path);
        ListObjectsV2Request request = ListObjectsV2Request.builder().bucket(bucketName)
            .prefix(prefix).delimiter(PATH_DELIMITER).maxKeys(1).build();

        CompletableFuture<ListObjectsV2Response> future = s3Client.listObjectsV2(request);
        ListObjectsV2Response response;

        try {
            response = future.get();
        } catch (Throwable e) {
            throw new IOException("Occur error in isDirectory, path: " + path, e);
        }

        return !response.commonPrefixes().isEmpty() || (!response.contents().isEmpty()
            && !response.contents().get(0).key().equals(key));
    }

    @Override
    public boolean delete(Path path, boolean recursive) throws IOException {
        if (isFile(path)) {
            deleteFileOrDirectory(path, true);
            return true;
        }

        if (isDirectory(path)) {
            if (!recursive) {
                throw new IOException("path is a directory, but recursive is false, path: " + path);
            }
            deleteFileOrDirectory(path, false);
            return true;
        }

        return false;
    }

    private void deleteFileOrDirectory(Path path, boolean isFile) throws IOException {
        if (isFile) {
            deleteFile(filePathToKey(bucketName, path));
            return;
        }

        String prefix = dirPathToPrefix(bucketName, path);
        String continuationToken = null;

        ListObjectsV2Request.Builder builder = ListObjectsV2Request.builder().bucket(bucketName)
            .prefix(prefix).maxKeys(S3_MAX_RETURN_KEYS);

        do {
            if (continuationToken != null) {
                builder.continuationToken(continuationToken);
            }

            CompletableFuture<ListObjectsV2Response> future = s3Client.listObjectsV2(
                builder.build());
            ListObjectsV2Response response;
            try {
                response = future.get();
            } catch (Throwable e) {
                throw new IOException("Failed to list objects, dir path: " + path, e);
            }

            for (S3Object s3Object : response.contents()) {
                deleteFile(s3Object.key());
            }
            continuationToken = response.nextContinuationToken();
        } while (continuationToken != null);
    }

    public void deleteFile(String key) throws IOException {
        CompletableFuture<DeleteObjectResponse> future = s3Client.deleteObject(
            DeleteObjectRequest.builder().bucket(bucketName).key(key).build());

        try {
            future.get();
        } catch (Throwable e) {
            throw new IOException(
                "Failed to delete file, file path: " + keyToFilePath(bucketName, key), e);
        }
    }

    @Override
    public boolean createNewFile(Path path) throws IOException {
        String key = filePathToKey(bucketName, path);

        if (exists(path)) {
            return false;
        }

        PutObjectRequest request = PutObjectRequest.builder().bucket(bucketName).key(key).build();

        CompletableFuture<PutObjectResponse> future = s3Client.putObject(request,
            AsyncRequestBody.fromBytes(new byte[0]));

        try {
            future.get();
        } catch (Throwable e) {
            throw new IOException("Fail to create a new file, file path: " + path, e);
        }

        return true;
    }

    @Override
    public long getFileSize(Path path) throws IOException {
        return getFileInfo(path).getLength();
    }

    @Override
    public long getFileCount(Path path) throws IOException {
        String key = filePathToKey(bucketName, path);
        long fileCount = 0;
        String continuationToken = null;

        ListObjectsV2Request.Builder builder = ListObjectsV2Request.builder().bucket(bucketName)
            .prefix(key);

        do {
            if (continuationToken != null) {
                builder.continuationToken(continuationToken);
            }

            CompletableFuture<ListObjectsV2Response> future = s3Client.listObjectsV2(
                builder.build());
            ListObjectsV2Response response;

            try {
                response = future.get();
            } catch (Throwable e) {
                throw new IOException("Failed to get content summary, dir path: " + path, e);
            }

            fileCount += response.contents().size();
            continuationToken = response.nextContinuationToken();
        } while (continuationToken != null);

        return fileCount;
    }

    @Override
    public FileInfo getFileInfo(Path path) throws IOException {
        String key = filePathToKey(bucketName, path);

        CompletableFuture<ListObjectsV2Response> future = s3Client.listObjectsV2(
            ListObjectsV2Request.builder().bucket(bucketName).prefix(key).build());
        ListObjectsV2Response response;

        try {
            response = future.get();
        } catch (Throwable e) {
            throw new IOException("Failed to get file status, file path: " + path, e);
        }

        if (response.contents().isEmpty()) {
            throw new IOException("File not found: " + path);
        }
        S3Object s3Object = response.contents().get(0);

        if (!s3Object.key().equals(key)) {
            throw new IOException("File not found: " + path);
        }

        return FileInfo.of().withPath(path).withLength(s3Object.size())
            .withModifiedTime(s3Object.lastModified().toEpochMilli());
    }

    @Override
    public FileInfo[] listFileInfo(Path path) throws IOException {
        return listFileInfo(path, null);
    }

    @Override
    public FileInfo[] listFileInfo(Path path, PathFilter filter) throws IOException {
        String prefix = dirPathToPrefix(bucketName, path);

        List<FileInfo> fileInfoList = new ArrayList<>();
        String continuationToken = null;

        ListObjectsV2Request.Builder builder = ListObjectsV2Request.builder().bucket(bucketName)
            .prefix(prefix).delimiter(PATH_DELIMITER).maxKeys(S3_MAX_RETURN_KEYS);

        do {
            if (continuationToken != null) {
                builder.continuationToken(continuationToken);
            }

            CompletableFuture<ListObjectsV2Response> future = s3Client.listObjectsV2(
                builder.build());
            ListObjectsV2Response response;

            try {
                response = future.get();
            } catch (Throwable e) {
                throw new IOException("Failed to list file status, dir path: " + path, e);
            }

            for (S3Object s3Object : response.contents()) {
                Path filePath = keyToFilePath(bucketName, s3Object.key());
                if (filter == null || filter.accept(filePath)) {
                    fileInfoList.add(FileInfo.of().withPath(filePath).withLength(s3Object.size())
                        .withModifiedTime(s3Object.lastModified().toEpochMilli()));
                }
            }

            for (CommonPrefix commonPrefix : response.commonPrefixes()) {
                Path dirPath = keyToFilePath(bucketName, commonPrefix.prefix());
                if (filter == null || filter.accept(dirPath)) {
                    fileInfoList.add(
                        FileInfo.of().withPath(dirPath).withLength(0).withModifiedTime(0));
                }
            }
            continuationToken = response.nextContinuationToken();
        } while (continuationToken != null);

        return fileInfoList.toArray(new FileInfo[0]);
    }


    @Override
    public void copyFromLocalFile(Path local, Path remote) throws IOException {
        String key = filePathToKey(bucketName, remote);
        java.nio.file.Path fromPath = Paths.get(local.toString());

        int count = 0;
        int maxTries = 3;
        File localFile = new File(local.toString());
        long localFileLen = localFile.length();
        while (true) {
            long start = System.currentTimeMillis();

            UploadFileRequest uploadFileRequest = UploadFileRequest.builder()
                .putObjectRequest(req -> req.bucket(bucketName).key(key)).source(fromPath).build();
            FileUpload upload = transferManager.uploadFile(uploadFileRequest);

            try {
                upload.completionFuture().get();
                LOGGER.info("upload to s3: size {} KB took {} ms {} -> {}",
                    String.format("%.1f", localFileLen / 1024.0),
                    System.currentTimeMillis() - start, local, remote);
                return;
            } catch (Throwable ex) {
                if (++count == maxTries) {
                    LOGGER.error("failed to upload file, from: {}, to: {}" + local, remote, ex);
                    throw new RuntimeException("failed to upload file", ex);
                }
            }
        }
    }

    @Override
    public void copyToLocalFile(Path remote, Path local) throws IOException {
        int count = 0;
        int maxTries = 3;
        FileInfo fileInfo = getFileInfo(remote);

        java.nio.file.Path toPath = Paths.get(local.toString());
        String key = filePathToKey(bucketName, remote);

        DownloadFileRequest downloadFileRequest = DownloadFileRequest.builder().getObjectRequest(
            req -> req.bucket(bucketName).key(key)
                .checksumMode(ChecksumMode.UNKNOWN_TO_SDK_VERSION)).destination(toPath).build();

        while (true) {
            FileDownload download = transferManager.downloadFile(downloadFileRequest);

            try {
                download.completionFuture().get();
                File localFile = new File(local.toString());
                if (localFile.length() != fileInfo.getLength()) {
                    LOGGER.warn("download from s3: size not same {} -> {}", remote, local);
                    if (++count == maxTries) {
                        return;
                    }
                } else {
                    LOGGER.info("download from dfs: {} -> {}", remote, local);
                    break;
                }
            } catch (Throwable ex) {
                if (++count == maxTries) {
                    LOGGER.error("failed to download file, from: {}, to: {}", remote, local, ex);
                    throw new RuntimeException("failed to download file", ex);
                }
            }
        }
    }

    @Override
    public boolean renameFile(Path from, Path to) throws IOException {
        String srcKey = filePathToKey(bucketName, from);
        String destKey = filePathToKey(bucketName, to);
        String filePathStr =
            LOCAL_RENAME_DIR + PATH_DELIMITER + keyToFilePath(bucketName, srcKey).toString()
                .replace(PATH_DELIMITER, LOCAL_RENAME_DELIMITER);
        Path localPath = new Path(filePathStr);

        copyToLocalFile(from, localPath);
        try {
            copyFromLocalFile(localPath, new Path(destKey));
        } finally {
            Files.delete(Paths.get(localPath.toString()));
        }
        deleteFile(srcKey);

        return true;
    }

    @Override
    public InputStream open(Path path) throws IOException {
        return new S3InputStream(s3Client, bucketName, filePathToKey(bucketName, path),
            inputStreamChunkSize);
    }

    @Override
    public void close() throws IOException {
        s3Client.close();
    }

    @Override
    public PersistentType getPersistentType() {
        return PersistentType.S3;
    }

    @VisibleForTesting
    public S3AsyncClient getS3Client() {
        return s3Client;
    }

    @VisibleForTesting
    public void setBucketName(String bucketName) {
        this.bucketName = bucketName;
    }
}
