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

package org.apache.geaflow.infer.util;

import static org.apache.geaflow.common.config.keys.ExecutionConfigKeys.JOB_WORK_PATH;
import static org.apache.geaflow.file.FileConfigKeys.USER_NAME;

import com.google.common.base.Preconditions;
import com.google.common.io.Resources;
import java.io.File;
import java.io.FileFilter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.stream.Collectors;
import org.apache.commons.io.FileUtils;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.common.config.keys.ExecutionConfigKeys;
import org.apache.geaflow.common.exception.GeaflowRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class InferFileUtils {

    private static final Logger LOGGER = LoggerFactory.getLogger(InferFileUtils.class);

    public static final String INFER_DIRECTORY = "infer";

    private static final String FILE_SUFFIX = "/";

    private static final String JAR_FILE = "jar:file:";

    private static final String UDF_RESOURCE_PATH = ".";

    public static final String PY_FILE_EXTENSION = ".py";

    public static final String JAR_FILE_EXTENSION = ".jar";

    private static final int DEFAULT_BUFFER_SIZE = 1024;

    public static final String REQUIREMENTS_TXT = "requirements.txt";

    public static void releaseLock(FileLock fileLock) {
        try {
            fileLock.release();
            fileLock.channel().close();
        } catch (IOException e) {
            LOGGER.error("release file lock failed", e);
        }
    }

    public static FileLock addLock(File file) {
        try {
            FileChannel channel = FileChannel.open(file.toPath(), StandardOpenOption.WRITE);
            return channel.lock();
        } catch (Exception e) {
            LOGGER.error("add lock file {} error", file.toPath(), e);
            throw new GeaflowRuntimeException("get file lock failed", e);
        }
    }

    public static void forceMkdir(File directory) throws IOException {
        String message;
        if (directory.exists()) {
            if (!directory.isDirectory()) {
                message = String.format("File %s exists and is not a directory. Unable to "
                    + "create directory.", directory);
                throw new IOException(message);
            }
        } else if (!directory.mkdirs() && !directory.isDirectory()) {
            message = "Unable to create directory " + directory;
            throw new IOException(message);
        }
    }

    public static String copyPythonFile(String parentDir, File resourceFile) {
        parentDir = parentDir.endsWith(FILE_SUFFIX) ? parentDir : parentDir + FILE_SUFFIX;
        File targetFile = new File(parentDir + resourceFile.getName());
        if (targetFile.exists()) {
            targetFile.delete();
            LOGGER.info("{} file is existed, delete", targetFile.getAbsolutePath());
        }
        try {
            FileUtils.copyFile(resourceFile, targetFile);
        } catch (Exception e) {
            throw new GeaflowRuntimeException(
                String.format("prepare python file [%s] failed, %s", resourceFile.getName(),
                    e.getMessage()));
        }
        LOGGER.info("prepare python file [{}] finish", resourceFile.getName());
        Preconditions.checkState(targetFile.exists());
        return targetFile.getAbsolutePath();
    }


    public static String copyInferFileByURL(String parentPath, String resourceFilePath) {
        File sourceFile = new File(resourceFilePath.trim());
        parentPath = parentPath.endsWith(FILE_SUFFIX) ? parentPath : parentPath + FILE_SUFFIX;
        File file = new File(parentPath + sourceFile.getName());
        if (file.exists()) {
            file.delete();
            LOGGER.info("{} file is existed, delete", file.getAbsolutePath());
        }
        try {
            URL url = InferFileUtils.class.getClassLoader().getResource(resourceFilePath);
            Preconditions.checkNotNull(url,
                String.format("Cannot find resource file [%s] in " + "classpath", resourceFilePath));
            File tmpFile = new File(file.getParent() + File.separator + "tmp_" + file.getName());
            FileUtils.copyURLToFile(url, tmpFile);
            tmpFile.renameTo(file);
        } catch (Exception e) {
            throw new GeaflowRuntimeException(
                String.format("prepare python file [%s] failed by url, %s", resourceFilePath,
                    e.getMessage()));
        }
        LOGGER.info("prepare python file [{}] finish by url", resourceFilePath);
        return file.getAbsolutePath();
    }

    public static String copyPythonFile(String parentDir, File resourceFile, String reName) {
        parentDir = parentDir.endsWith(FILE_SUFFIX) ? parentDir : parentDir + FILE_SUFFIX;
        File targetFile = new File(parentDir + reName);
        if (targetFile.exists()) {
            targetFile.delete();
            LOGGER.info("{} file is existed, delete", targetFile.getAbsolutePath());
        }
        try {
            FileUtils.copyFile(resourceFile, targetFile);
        } catch (Exception e) {
            throw new GeaflowRuntimeException(
                String.format("prepare python file [%s] and rename [%s] failed, %s",
                    resourceFile.getName(), reName, e.getMessage()));
        }
        LOGGER.info("prepare python file [{}] and rename [{}] finish", resourceFile.getName(),
            reName);
        Preconditions.checkState(targetFile.exists());
        return targetFile.getAbsolutePath();
    }

    public static List<File> getPythonFilesByCondition(FileFilter fileFilter) {
        String jobPackagePath = Resources.getResource(UDF_RESOURCE_PATH).getPath();
        File folder = new File(jobPackagePath);
        for (File file : folder.listFiles()) {
            LOGGER.info("folder {} sub file {}", folder.getAbsolutePath(), file.getName());
        }
        File[] subFiles = folder.listFiles(fileFilter);
        if (subFiles == null) {
            return Collections.emptyList();
        }
        return Arrays.asList(subFiles);
    }

    public static File getUserJobJarFile() {
        String jobPackagePath = Resources.getResource(UDF_RESOURCE_PATH).getPath();
        File folder = new File(jobPackagePath);
        for (File file : folder.listFiles()) {
            if (file.isFile() && file.getName().endsWith(JAR_FILE_EXTENSION)) {
                LOGGER.info("folder {} user job jar is {}", folder.getAbsolutePath(),
                    file.getName());
                return file;
            }
        }
        return null;
    }


    public static String getInferDirectory(Configuration configuration) {
        String workPath = configuration.getString(JOB_WORK_PATH);
        String userName = configuration.getString(USER_NAME);
        String inferPath = workPath + File.separator + userName + File.separator + INFER_DIRECTORY;
        String jobUniqueId = configuration.getString(ExecutionConfigKeys.JOB_UNIQUE_ID);
        File inferFile = new File(inferPath + File.separator + jobUniqueId);
        if (!inferFile.exists()) {
            inferFile.mkdirs();
        }
        return inferFile.getAbsolutePath();
    }

    public static String createTargetDir(String dirName, Configuration configuration) {
        String inferDirectory = getInferDirectory(configuration);
        File userFilesDirFile = new File(inferDirectory, dirName);
        if (!userFilesDirFile.exists()) {
            userFilesDirFile.mkdirs();
        }
        String absolutePath = userFilesDirFile.getAbsolutePath();
        LOGGER.info("create infer directory is {}", absolutePath);
        return absolutePath;
    }

    public static List<Path> getPathsFromResourceJAR(String folder) throws URISyntaxException, IOException {
        List<Path> result;
        String jarPath = InferFileUtils.class.getProtectionDomain()
            .getCodeSource()
            .getLocation()
            .toURI()
            .getPath();
        LOGGER.info("jar path {}", jarPath);
        URI uri = URI.create(JAR_FILE + jarPath);
        try (FileSystem fs = FileSystems.newFileSystem(uri, Collections.emptyMap())) {
            result = Files.walk(fs.getPath(folder))
                .filter(Files::isRegularFile)
                .collect(Collectors.toList());
        }
        return result;
    }

    public static void prepareInferFilesFromJars(String targetDirectory) {
        File userJobJarFile = getUserJobJarFile();
        if (userJobJarFile == null) {
            // In test or development environment, JAR file may not exist
            // This is acceptable - the system will initialize with random weights
            LOGGER.warn(
                "User job JAR file not found. Inference files will not be extracted from JAR. "
                + "System will initialize with default/random model weights.");
            return;
        }
        try {
            JarFile jarFile = new JarFile(userJobJarFile);
            Enumeration<JarEntry> entries = jarFile.entries();
            while (entries.hasMoreElements()) {
                JarEntry entry = entries.nextElement();
                String entryName = entry.getName();
                if (!entry.isDirectory()) {
                    String inferFile = extractFile(targetDirectory, entryName, entry, jarFile);
                    LOGGER.info("cp infer file {} to {} from jar file {}", entryName, inferFile, userJobJarFile.getName());
                } else {
                    File entryDestination = new File(targetDirectory, entry.getName());
                    if (!entryDestination.exists()) {
                        entryDestination.mkdirs();
                    }
                    LOGGER.info("create infer directory is {}", entryDestination);
                }
            }
            jarFile.close();
        } catch (IOException e) {
            LOGGER.error("open jar file {} failed", userJobJarFile.getName());
        }
    }


    private static String extractFile(String targetDirectory, String fileName, JarEntry entry,
                                      JarFile jarFile) throws IOException {
        String targetFilePath = targetDirectory + File.separator + fileName;
        try (InputStream inputStream = jarFile.getInputStream(entry);
             FileOutputStream outputStream = new FileOutputStream(targetFilePath)) {
            byte[] buffer = new byte[DEFAULT_BUFFER_SIZE];
            int bytesRead;
            while ((bytesRead = inputStream.read(buffer)) != -1) {
                outputStream.write(buffer, 0, bytesRead);
            }
        }
        return targetFilePath;
    }
}
