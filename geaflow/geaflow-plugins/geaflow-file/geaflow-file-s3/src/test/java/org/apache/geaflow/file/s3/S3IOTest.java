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

import static org.apache.geaflow.file.s3.S3IO.S3_MAX_RETURN_KEYS;
import static org.apache.geaflow.file.s3.S3IO.filePathToKey;

import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import org.apache.commons.io.IOUtils;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.common.utils.GsonUtil;
import org.apache.geaflow.file.FileConfigKeys;
import org.apache.geaflow.file.FileInfo;
import org.apache.geaflow.file.PersistentIOBuilder;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.services.s3.model.DeleteBucketRequest;
import software.amazon.awssdk.services.s3.model.DeleteBucketResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;
import software.amazon.awssdk.services.s3.model.S3Object;

public class S3IOTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(S3IOTest.class);

    private static S3IO s3IO;
    private LocalFileSystem localFileSystem;
    private String remoteRoot;
    private String localRoot;
    private String bucketName;

    @BeforeMethod
    public void setUp() throws IOException {
        String testName = "S3IOTest" + System.currentTimeMillis();
        bucketName = "geaflow-s3-io-test";
        remoteRoot = bucketName + "/" + testName;

        Map<String, String> config = new HashMap<>();
        config.put(FileConfigKeys.S3_BUCKET_NAME.getKey(), bucketName);

        // Fill the following configuration for s3 if needing to run S3IOTest.
        config.put(FileConfigKeys.S3_ENDPOINT.getKey(), "");
        config.put(FileConfigKeys.S3_ACCESS_KEY_ID.getKey(), "");
        config.put(FileConfigKeys.S3_ACCESS_KEY.getKey(), "");

        Configuration configuration = new Configuration();
        configuration.put(FileConfigKeys.PERSISTENT_TYPE, "S3");
        configuration.put(FileConfigKeys.JSON_CONFIG, GsonUtil.toJson(config));
        s3IO = (S3IO) PersistentIOBuilder.build(configuration);
        localFileSystem = FileSystem.getLocal(new org.apache.hadoop.conf.Configuration());
        localRoot = "/tmp/" + testName;
        localFileSystem.mkdirs(new Path(localRoot));
    }

    @AfterMethod
    public void tearDown() throws IOException {
        localFileSystem.delete(new Path(localRoot), true);
        s3IO.delete(new Path(remoteRoot), true);
        s3IO.close();
    }

    private void createNewFileWithFixedEmptyBytes(Path path, int bytesLen) throws IOException {
        String key = filePathToKey(bucketName, path);

        if (s3IO.exists(path)) {
            return;
        }

        PutObjectRequest request = PutObjectRequest.builder().bucket(bucketName).key(key).build();

        CompletableFuture<PutObjectResponse> future = s3IO.getS3Client()
            .putObject(request, AsyncRequestBody.fromBytes(new byte[bytesLen]));

        try {
            future.get();
        } catch (Throwable e) {
            throw new IOException("Fail to create a new file, file path: " + path, e);
        }
    }

    private void createNewFileWithFixedBytes(Path path, byte[] bytes) throws IOException {
        String key = filePathToKey(bucketName, path);

        if (s3IO.exists(path)) {
            return;
        }

        PutObjectRequest request = PutObjectRequest.builder().bucket(bucketName).key(key).build();

        CompletableFuture<PutObjectResponse> future = s3IO.getS3Client()
            .putObject(request, AsyncRequestBody.fromBytes(bytes));

        try {
            future.get();
        } catch (Throwable e) {
            throw new IOException("Fail to create a new file, file path: " + path, e);
        }
    }

    private void deleteBucket(String bucketName) throws IOException {
        DeleteBucketRequest request = DeleteBucketRequest.builder().bucket(bucketName).build();

        CompletableFuture<DeleteBucketResponse> future = s3IO.getS3Client().deleteBucket(request);

        try {
            future.get();
        } catch (Throwable e) {
            throw new IOException("Fail to create a new file, file path: " + bucketName, e);
        }
    }

    @Test(enabled = false)
    void testCreateBucket() throws IOException {
        String testName = getTestName();
        String newBucketName =
            "geaflow-" + testName.toLowerCase() + "-" + System.currentTimeMillis();
        s3IO.setBucketName(newBucketName);
        s3IO.checkAndCreateBucket();
        Path path = new Path(testName + System.currentTimeMillis() + ".txt");
        s3IO.createNewFile(path);
        s3IO.deleteFile(path.toString());

        deleteBucket(newBucketName);
        s3IO.setBucketName(bucketName);
    }

    @Test(enabled = false)
    void testCreateAndDelete() throws IOException {
        String testName = getTestName();
        String testFileName = testName + ".txt";
        String pathStr = remoteRoot + "/" + testName + "/s3/1/" + testFileName;
        Path filePath = new Path(pathStr);

        Assert.assertTrue(s3IO.createNewFile(filePath));
        Assert.assertFalse(s3IO.createNewFile(filePath));
        Assert.assertTrue(s3IO.delete(filePath, false));
        s3IO.createNewFile(filePath);
        Assert.assertTrue(s3IO.delete(filePath, true));
        Assert.assertFalse(s3IO.exists(filePath));

        pathStr = remoteRoot + "//" + testName + "/s3///1//" + testFileName + "/";
        filePath = new Path(pathStr);
        Assert.assertTrue(s3IO.createNewFile(filePath));
        Assert.assertTrue(s3IO.delete(filePath, true));
        Assert.assertFalse(s3IO.exists(filePath));
    }

    @Test(enabled = false)
    void testGetFileCount() throws IOException {
        String testName = getTestName();
        String root = remoteRoot + "/" + testName;
        Path testFilePath1 = new Path(root + "/s3/1/" + testName + "1.txt");
        Path testFilePath2 = new Path(root + "/s3/1/" + testName + "2.txt");
        Path testFilePath3 = new Path(root + "/s3/2/" + testName + "1.txt");
        Path testFilePath4 = new Path(root + "/s3/3/" + testName + "1.txt");
        Path testFilePath5 = new Path(root + "/s3/4/" + testName + "3.txt");
        Path testFilePath6 = new Path(root + "/s3/" + testName + ".txt");

        s3IO.createNewFile(testFilePath1);
        s3IO.createNewFile(testFilePath2);
        s3IO.createNewFile(testFilePath3);
        s3IO.createNewFile(testFilePath4);
        s3IO.createNewFile(testFilePath5);
        s3IO.createNewFile(testFilePath6);

        Assert.assertEquals(s3IO.getFileCount(new Path(root)), 6);
    }

    @Test(enabled = false)
    void testDeleteRecursive() throws IOException {
        String testName = getTestName();
        Path testFilePath1 = new Path(remoteRoot + "/" + testName + "/s3/" + testName + "1.txt");
        Path testFilePath2 = new Path(remoteRoot + "/" + testName + "/s3/" + testName + "2.txt");
        Path testFilePath3 = new Path(remoteRoot + "/" + testName + "/s3/1/" + testName + "1.txt");
        Path testFilePath4 = new Path(remoteRoot + "/" + testName + "/s3/1/" + testName + "2.txt");
        Path testFilePath5 = new Path(remoteRoot + "/" + testName + "/s3/2/" + testName + "1.txt");
        Path testFilePath6 = new Path(remoteRoot + "/" + testName + "/s3/2/" + testName + "2.txt");
        Path testFilePath7 = new Path(remoteRoot + "/" + testName + "/s3/2/" + testName + "3.txt");
        Path testFilePath8 = new Path(
            remoteRoot + "/" + testName + "/s3/2/1/" + testName + "1.txt");

        s3IO.createNewFile(testFilePath1);
        s3IO.createNewFile(testFilePath2);
        s3IO.createNewFile(testFilePath3);
        s3IO.createNewFile(testFilePath4);
        s3IO.createNewFile(testFilePath5);
        s3IO.createNewFile(testFilePath6);
        s3IO.createNewFile(testFilePath7);
        s3IO.createNewFile(testFilePath8);

        Path rootPath = new Path(remoteRoot);
        Path dirPath1 = new Path(remoteRoot + "/" + testName + "/s3/1/");
        Path dirPath2 = new Path(remoteRoot + "/" + testName + "/s3");

        Assert.assertEquals(s3IO.getFileCount(rootPath), 8);
        Assert.assertFalse(s3IO.delete(new Path(testName + "/" + "s3_1"), true));
        Assert.assertFalse(s3IO.delete(new Path(testName + "/" + "s3_1"), false));
        Assert.assertFalse(
            s3IO.delete(new Path(remoteRoot + "/" + testName + "/s3/1/" + testName + "3.txt"),
                true));
        Assert.assertFalse(
            s3IO.delete(new Path(remoteRoot + "/" + testName + "/s3/1/" + testName + "3.txt"),
                false));

        Assert.assertEquals(s3IO.getFileCount(rootPath), 8);

        try {
            s3IO.delete(dirPath1, false);
        } catch (IOException e) {
            Assert.assertTrue(e.getMessage().contains("path is a directory, but " + "recursive"));
        }

        Assert.assertTrue(s3IO.delete(testFilePath5, true));
        Assert.assertEquals(s3IO.getFileCount(rootPath), 7);
        Assert.assertTrue(s3IO.delete(testFilePath6, false));
        Assert.assertEquals(s3IO.getFileCount(rootPath), 6);

        Assert.assertTrue(s3IO.delete(dirPath1, true));
        Assert.assertEquals(s3IO.getFileCount(rootPath), 4);

        Assert.assertTrue(s3IO.delete(dirPath2, true));
        Assert.assertEquals(s3IO.getFileCount(rootPath), 0);
    }

    @Test(enabled = false)
    void testGetFileInfo() throws IOException {
        String testName = getTestName();
        Path testFilePath = new Path(remoteRoot + "/" + testName + "/s3/1/" + testName + "1.txt");
        createNewFileWithFixedEmptyBytes(testFilePath, 5);
        FileInfo fileInfo = s3IO.getFileInfo(testFilePath);
        Assert.assertEquals(fileInfo.getLength(), 5);
        Assert.assertTrue(fileInfo.getModificationTime() != 0);
        Assert.assertEquals(fileInfo.getPath(), testFilePath);

        LOGGER.info("file {} last modify time {}", testFilePath, fileInfo.getModificationTime());
    }

    @Test(enabled = false)
    void testListFileInfo() throws IOException {
        String testName = getTestName();
        String root = remoteRoot + "/" + testName;
        Set<Path> filePathSet1 = new HashSet<>();
        Set<Path> filePathSet2 = new HashSet<>();
        Set<Path> dirPathSet = new HashSet<>();

        Path testFilePath1 = new Path(root + "/s3/" + testName + "1.txt");
        Path testFilePath2 = new Path(root + "/s3/" + testName + "2.txt");
        filePathSet1.add(testFilePath1);
        filePathSet1.add(testFilePath2);

        Path testFilePath3 = new Path(root + "/s3/1/" + testName + "1.txt");
        Path testFilePath4 = new Path(root + "/s3/2/" + testName + "1.txt");
        Path testFilePath5 = new Path(root + "/s3/3/" + testName + "1.txt");
        dirPathSet.add(new Path(root + "/s3/1/"));
        dirPathSet.add(new Path(root + "/s3/2/"));
        dirPathSet.add(new Path(root + "/s3/3/"));
        filePathSet2.add(testFilePath3);
        filePathSet2.add(testFilePath4);
        filePathSet2.add(testFilePath5);

        createNewFileWithFixedEmptyBytes(testFilePath1, 3);
        createNewFileWithFixedEmptyBytes(testFilePath2, 3);
        createNewFileWithFixedEmptyBytes(testFilePath3, 3);
        createNewFileWithFixedEmptyBytes(testFilePath4, 3);
        createNewFileWithFixedEmptyBytes(testFilePath5, 3);

        Path dirPath1 = new Path(root + "/s3/1/");
        Path dirPath2 = new Path(root + "/s3");

        FileInfo[] fileInfos = s3IO.listFileInfo(dirPath2);
        int fileCount = 0;
        int dirCount = 0;
        for (FileInfo fileInfo : fileInfos) {
            if (fileInfo.getLength() != 0) {
                Assert.assertEquals(fileInfo.getLength(), 3);
                Assert.assertTrue(fileInfo.getModificationTime() != 0);
                Assert.assertTrue(filePathSet1.contains(fileInfo.getPath()));
                fileCount++;
            } else {
                Assert.assertEquals(fileInfo.getLength(), 0);
                Assert.assertEquals(fileInfo.getModificationTime(), 0);
                Assert.assertTrue(dirPathSet.contains(fileInfo.getPath()));
                dirCount++;
            }
        }
        Assert.assertEquals(fileCount, 2);
        Assert.assertEquals(dirCount, 3);
        List<String> fileNames = s3IO.listFileName(dirPath2);
        Assert.assertEquals(fileNames.size(), 5);
        Assert.assertTrue(fileNames.contains("testListFileInfo1.txt"));

        fileInfos = s3IO.listFileInfo(dirPath1);
        fileCount = 0;
        dirCount = 0;
        for (FileInfo fileInfo : fileInfos) {
            if (fileInfo.getLength() != 0) {
                Assert.assertEquals(fileInfo.getLength(), 3);
                Assert.assertTrue(fileInfo.getModificationTime() != 0);
                Assert.assertTrue(filePathSet2.contains(fileInfo.getPath()));
                fileCount++;
            } else {
                dirCount++;
            }
        }
        Assert.assertEquals(fileCount, 1);
        Assert.assertEquals(dirCount, 0);
    }

    @Test(enabled = false)
    public void testIsFileAndIsDirectory() throws IOException {
        String testName = getTestName();
        String root = remoteRoot + "/" + testName;
        Path testFilePath = new Path(root + "/s3/1/" + testName + "1.txt");
        createNewFileWithFixedEmptyBytes(testFilePath, 3);

        Path filePath1 = new Path(root + "/s3/1/" + testName + "2.txt");

        Path dirPath1 = new Path(root + "/s3/1/");
        Path dirPath2 = new Path(root + "/s3/");
        Path dirPath3 = new Path(root);
        Path dirPath4 = new Path(remoteRoot);

        Assert.assertTrue(s3IO.isFile(testFilePath));
        Assert.assertFalse(s3IO.isFile(filePath1));
        Assert.assertFalse(s3IO.isFile(dirPath1));

        Assert.assertFalse(s3IO.isDirectory(testFilePath));
        Assert.assertFalse(s3IO.isDirectory(filePath1));
        Assert.assertTrue(s3IO.isDirectory(dirPath1));
        Assert.assertTrue(s3IO.isDirectory(dirPath2));
        Assert.assertTrue(s3IO.isDirectory(dirPath3));
        Assert.assertTrue(s3IO.isDirectory(dirPath4));

        Assert.assertTrue(s3IO.exists(testFilePath));
        Assert.assertTrue(s3IO.exists(dirPath1));
        Assert.assertTrue(s3IO.exists(dirPath2));
        Assert.assertTrue(s3IO.exists(dirPath3));
        Assert.assertTrue(s3IO.exists(dirPath4));
        Assert.assertFalse(s3IO.exists(filePath1));
    }

    @Test(enabled = false)
    public void testRename() throws IOException {
        String testName = getTestName();
        String root = remoteRoot + "/" + testName;
        Path oldFilePath = new Path(root + "/s3/1/" + testName + "1.txt");
        Path newFilePath = new Path(root + "/s3/1/" + testName + "2.txt");
        createNewFileWithFixedEmptyBytes(oldFilePath, 3);
        Assert.assertTrue(s3IO.exists(oldFilePath));
        Assert.assertFalse(s3IO.exists(newFilePath));

        s3IO.renameFile(oldFilePath, newFilePath);
        Assert.assertTrue(s3IO.exists(newFilePath));
        Assert.assertFalse(s3IO.exists(oldFilePath));
    }

    @Test(enabled = false)
    public void testCopyFromLocal() throws IOException {
        String testName = getTestName();
        String root = remoteRoot + "/" + testName;
        Path localFilePath = new Path(localRoot + "/" + testName + "1.txt");
        RandomAccessFile file = new RandomAccessFile(localFilePath.toUri().getPath(), "rw");
        int fileLen = 1024 * 1024;
        byte[] bytes = new byte[fileLen];
        Random random = new Random();
        random.nextBytes(bytes);
        file.write(bytes);
        file.close();

        Path remoteFilePath = new Path(root + "/s3/1/" + testName + "1" + ".txt");

        s3IO.copyFromLocalFile(localFilePath, remoteFilePath);
        Assert.assertEquals(s3IO.getFileSize(remoteFilePath), fileLen);
        InputStream in = s3IO.open(remoteFilePath);
        byte[] readBytes = IOUtils.toByteArray(in);
        Assert.assertEquals(readBytes, bytes);
        in.close();
        in = s3IO.open(remoteFilePath);

        for (int i = 0; i < 100; i++) {
            readBytes[i] = (byte) in.read();
        }
        int readFileLen = in.read(readBytes, 100, 2 * fileLen);
        Assert.assertEquals(readBytes, bytes);
        Assert.assertEquals(readFileLen, 1024 * 1024 - 100);
    }

    @Test(enabled = false)
    public void testCopyToLocal() throws IOException {
        String testName = getTestName();
        String root = remoteRoot + "/" + testName;
        Path localFilePath = new Path(localRoot + "/" + testName + "1.txt");
        Path remoteFilePath = new Path(root + "/s3/1/" + testName + "1" + ".txt");

        int fileLen = 1024 * 1024;
        byte[] bytes = new byte[fileLen];
        Random random = new Random();
        random.nextBytes(bytes);

        createNewFileWithFixedBytes(remoteFilePath, bytes);

        s3IO.copyToLocalFile(remoteFilePath, localFilePath);
        Assert.assertEquals(s3IO.getFileSize(remoteFilePath), fileLen);
        Assert.assertEquals(localFileSystem.getFileStatus(localFilePath).getLen(), fileLen);

        FSDataInputStream fsDataInputStream = localFileSystem.open(localFilePath);
        byte[] readBytes = new byte[fileLen];
        int readLen = fsDataInputStream.read(readBytes);
        Assert.assertEquals(readLen, fileLen);
        Assert.assertEquals(readBytes, bytes);
    }

    // This test is normally closed and is used to delete all data in test bucket.
    @Test(enabled = false)
    public void deleteAllObjects() {
        String continuationToken = null;

        ListObjectsV2Request.Builder listRequestBuilder = ListObjectsV2Request.builder()
            .bucket(bucketName).maxKeys(S3_MAX_RETURN_KEYS);

        do {
            if (continuationToken != null) {
                listRequestBuilder.continuationToken(continuationToken);
            }

            CompletableFuture<ListObjectsV2Response> future = s3IO.getS3Client()
                .listObjectsV2(listRequestBuilder.build());
            ListObjectsV2Response response;
            try {
                response = future.get();
            } catch (Throwable e) {
                throw new RuntimeException("Failed to list objects in bucket: " + bucketName, e);
            }

            for (S3Object s3Object : response.contents()) {
                try {
                    s3IO.deleteFile(s3Object.key());
                } catch (IOException e) {
                    throw new RuntimeException(
                        "Failed to delete object in bucket: " + bucketName + ", object key: "
                            + s3Object.key(), e);
                }
            }

            continuationToken = response.nextContinuationToken();
        } while (continuationToken != null);
    }

    private static String getTestName() {
        return Thread.currentThread().getStackTrace()[2].getMethodName();
    }
}
