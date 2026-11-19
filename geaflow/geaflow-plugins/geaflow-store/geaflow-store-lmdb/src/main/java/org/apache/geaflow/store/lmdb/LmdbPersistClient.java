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

package org.apache.geaflow.store.lmdb;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor.DiscardOldestPolicy;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.common.config.keys.StateConfigKeys;
import org.apache.geaflow.common.errorcode.RuntimeErrors;
import org.apache.geaflow.common.exception.GeaflowRuntimeException;
import org.apache.geaflow.common.thread.Executors;
import org.apache.geaflow.common.tuple.Tuple;
import org.apache.geaflow.file.FileConfigKeys;
import org.apache.geaflow.file.FileInfo;
import org.apache.geaflow.file.IPersistentIO;
import org.apache.geaflow.file.PersistentIOBuilder;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LmdbPersistClient {

    private static final Logger LOGGER = LoggerFactory.getLogger(LmdbPersistClient.class);

    private static final String COMMIT_TAG_FILE = "_commit";
    private static final String FILES = "FILES";
    private static final int DELETE_CAPACITY = 64;
    private static final String DATAS = "datas";
    private static final String META = "meta";
    private static final String FILE_SEPARATOR = ",";
    private static final String SST_SUFFIX = "sst";

    private final Long persistTimeout;
    private final IPersistentIO persistIO;
    private final NavigableMap<Long, CheckPointFileInfo> checkPointFileInfo;
    private final ExecutorService copyFileService;
    private final ExecutorService deleteFileService;
    private final ExecutorService backgroundDeleteService = new ThreadPoolExecutor(
        1, 1, 300L, TimeUnit.SECONDS, new LinkedBlockingQueue<>(1),
        new BasicThreadFactory.Builder().namingPattern("asyncDeletes-%d").daemon(true).build(), new DiscardOldestPolicy());

    public LmdbPersistClient(Configuration configuration) {
        this.persistIO = PersistentIOBuilder.build(configuration);
        this.checkPointFileInfo = new ConcurrentSkipListMap<>();
        int persistThreadNum = configuration.getInteger(FileConfigKeys.PERSISTENT_THREAD_SIZE);
        int persistCleanThreadNum = configuration.getInteger(LmdbConfigKeys.ROCKSDB_PERSISTENT_CLEAN_THREAD_SIZE);
        this.persistTimeout = (long) configuration.getInteger(
            StateConfigKeys.STATE_ROCKSDB_PERSIST_TIMEOUT_SECONDS);
        copyFileService = Executors.getExecutorService(1, persistThreadNum, "persist-%d");
        deleteFileService = Executors.getService(persistCleanThreadNum, DELETE_CAPACITY, 300L, TimeUnit.SECONDS);
        ((ThreadPoolExecutor) deleteFileService).setRejectedExecutionHandler(new ThreadPoolExecutor.DiscardOldestPolicy());
    }

    public void clearFileInfo() {
        checkPointFileInfo.clear();
    }

    public long getSstIndex(String filename) {
        try {
            return Long.parseLong(filename.substring(0, filename.indexOf(LmdbConfigKeys.FILE_DOT)));
        } catch (Throwable ignore) {
            LOGGER.warn("filename {} is abnormal", filename);
            return 0;
        }
    }

    private long getMetaFileId(String fileName) {
        return Long.parseLong(fileName.substring(fileName.indexOf(LmdbConfigKeys.FILE_DOT) + 1));
    }

    private static String getMetaFileName(long chkId) {
        return META + LmdbConfigKeys.FILE_DOT + chkId;
    }

    public void archive(long chkId, String localChkPath, String remotePath,
                        long keepCheckpointNum) throws Exception {
        Set<String> lastFullFiles = getLastFullFiles(chkId, localChkPath, remotePath);

        CheckPointFileInfo currentFileInfo = new CheckPointFileInfo(chkId);
        List<Callable<Long>> callers = new ArrayList<>();

        File localChkFile = new File(localChkPath);
        String[] sstFileNames = localChkFile.list((dir, name) -> name.endsWith(SST_SUFFIX));
        FileUtils.write(FileUtils.getFile(localChkFile, FILES), Joiner.on(FILE_SEPARATOR).join(sstFileNames),
            Charset.defaultCharset());

        // copy sst files.
        long size = 0L;
        String dataPath = Paths.get(remotePath, DATAS).toString();
        for (String subFileName : sstFileNames) {
            currentFileInfo.addFullFile(subFileName);
            if (!lastFullFiles.contains(subFileName)) {
                currentFileInfo.addIncDataFile(subFileName);
                File tmp = FileUtils.getFile(localChkFile, subFileName);
                callers.add(copyFromLocal(new Path(tmp.getAbsolutePath()),
                    new Path(dataPath, subFileName), tmp.length()));
                size = size + tmp.length();
            }
        }
        String[] metaFileNames = localChkFile.list((dir, name) -> !name.endsWith(SST_SUFFIX));
        String metaPath = Paths.get(remotePath, getMetaFileName(chkId)).toString();
        for (String metaFileName : metaFileNames) {
            File tmp = FileUtils.getFile(localChkFile, metaFileName);
            callers.add(copyFromLocal(new Path(tmp.getAbsolutePath()),
                new Path(metaPath, metaFileName), tmp.length()));
            size = size + tmp.length();
        }
        LOGGER.info("checkpointId {}, full {}, lastFullFiles {}, currentIncre {}", chkId,
            Arrays.toString(sstFileNames), lastFullFiles, currentFileInfo.getIncDataFiles());

        final long startTime = System.nanoTime();
        completeHandler(callers, copyFileService);
        callers.clear();
        persistIO.createNewFile(new Path(metaPath, COMMIT_TAG_FILE));
        double costMs = (System.nanoTime() - startTime) / 1000000.0;

        LOGGER.info(
            "RocksDB {} archive local:{} to {} (incre[{}]/full[{}]) took {}ms. incre data size {}KB, speed {}KB/s {}",
            persistIO.getPersistentType(), localChkFile.getAbsolutePath(), remotePath,
            currentFileInfo.getIncDataFiles().size(), currentFileInfo.getFullDataFiles().size(),
            costMs, size / 1024, size * 1000 / (1024 * costMs),
            currentFileInfo.getIncDataFiles().toString());

        checkPointFileInfo.put(chkId, currentFileInfo);

        backgroundDeleteService.execute(() ->
            cleanLocalAndRemoteFiles(chkId, remotePath, keepCheckpointNum, localChkFile));
    }


    public long getLatestCheckpointId(String remotePathStr) {
        try {
            if (!persistIO.exists(new Path(remotePathStr))) {
                return -1;
            }
            List<String> files = persistIO.listFileName(new Path(remotePathStr));
            List<Long> chkIds = files.stream().filter(f -> f.startsWith(META)).map(this::getMetaFileId)
                .filter(f -> f > 0).sorted(Collections.reverseOrder()).collect(Collectors.toList());
            LOGGER.info("find available chk {}", chkIds);
            for (Long chkId : chkIds) {
                String path = Paths.get(remotePathStr, getMetaFileName(chkId), COMMIT_TAG_FILE).toString();
                if (persistIO.exists(new Path(path))) {
                    return chkId;
                } else {
                    LOGGER.info("chk {} has no path {}", chkId, path);
                }
            }
        } catch (IOException e) {
            throw new GeaflowRuntimeException(RuntimeErrors.INST.stateRocksDbError("recover fail"), e);
        }
        return -1;
    }

    public void recover(long chkId, String localRdbPath, String localChkPath, String remotePathStr)
        throws Exception {
        checkPointFileInfo.clear();
        File rocksDBChkFile = new File(localChkPath);
        File rocksDBFile = new File(localRdbPath);
        LOGGER.info("delete {} {}", localChkPath, localRdbPath);
        FileUtils.deleteQuietly(rocksDBChkFile);
        FileUtils.deleteQuietly(rocksDBFile);

        rocksDBChkFile.mkdirs();
        rocksDBFile.mkdirs();

        final long startTime = System.currentTimeMillis();
        Path remotePath = new Path(remotePathStr);
        if (!persistIO.exists(remotePath)) {
            String msg = String.format("checkPoint: %s is not exist in remote", remotePath);
            LOGGER.warn(msg);
            throw new GeaflowRuntimeException(RuntimeErrors.INST.stateRocksDbError(msg));
        }

        // fetch manifests.
        String remoteMeta = Paths.get(remotePathStr, getMetaFileName(chkId)).toString();
        InputStream in = persistIO.open(new Path(remoteMeta, FILES));
        String sstString = IOUtils.toString(in, Charset.defaultCharset());
        List<String> list = Splitter.on(FILE_SEPARATOR).omitEmptyStrings().splitToList(sstString);

        CheckPointFileInfo commitedInfo = new CheckPointFileInfo(chkId);
        recoveryData(remotePath, rocksDBChkFile, commitedInfo, list, remoteMeta);
        LOGGER.info("recoveryFromRemote {} cost {}ms", remotePath,
            System.currentTimeMillis() - startTime);
        checkPointFileInfo.put(chkId, commitedInfo);

        for (File file : rocksDBChkFile.listFiles()) {
            Files.createLink(FileSystems.getDefault().getPath(localRdbPath, file.getName()), file.toPath());
        }

        backgroundDeleteService.execute(() -> cleanLocalChk(chkId, new File(localChkPath)));
    }

    private static void cleanLocalChk(long chkId, File localChkFile) {
        String chkPrefix = LmdbConfigKeys.getChkPathPrefix(localChkFile.getName());
        FilenameFilter filter = (dir, name) -> {
            if (LmdbConfigKeys.isChkPath(name) && name.startsWith(chkPrefix)) {
                return chkId > LmdbConfigKeys.getChkIdFromChkPath(name);
            } else {
                return false;
            }
        };
        File[] subFiles = localChkFile.getParentFile().listFiles(filter);
        for (File path : subFiles) {
            LOGGER.info("delete local chk {}", path.toURI());
            FileUtils.deleteQuietly(path);
        }
    }

    private Set<String> getLastFullFiles(long chkId, String localChkPath, String remotePath)
        throws IOException {
        CheckPointFileInfo commitFileInfo = checkPointFileInfo.get(chkId);
        if (commitFileInfo == null) {
            Entry<Long, CheckPointFileInfo> info = checkPointFileInfo.lowerEntry(chkId);
            if (info != null) {
                commitFileInfo = info.getValue();
            } else {
                Path path = new Path(remotePath);
                PathFilter filter = path1 -> path1.getName().startsWith(META);
                if (persistIO.exists(path)) {
                    FileInfo[] metaFileStatuses = persistIO.listFileInfo(path, filter);
                    Path lastMetaPath = getLastMetaFile(chkId, metaFileStatuses);
                    if (lastMetaPath != null) {
                        commitFileInfo = new CheckPointFileInfo(chkId);
                        commitFileInfo.addFullFiles(getKeptFileName(lastMetaPath));
                    }
                }
            }
        }

        Set<String> lastFullFiles;
        if (commitFileInfo != null) {
            lastFullFiles = new HashSet<>(commitFileInfo.getFullDataFiles());
        } else {
            lastFullFiles = new HashSet<>();
        }
        File file = new File(localChkPath);

        // current sst number must be larger than the last one.
        String[] curNames = file.list();
        Preconditions.checkNotNull(curNames, localChkPath + " is null");

        Optional<Long> chkLargestSst = Arrays.stream(curNames)
            .filter(c -> c.endsWith(SST_SUFFIX)).map(this::getSstIndex).max(Long::compareTo);
        Optional<Long> lastLargestSst = lastFullFiles.stream().filter(c -> c.endsWith(SST_SUFFIX))
            .map(this::getSstIndex).max(Long::compareTo);
        if (chkLargestSst.isPresent() && lastLargestSst.isPresent()) {
            Preconditions.checkArgument(chkLargestSst.get().compareTo(lastLargestSst.get()) >= 0,
                "%s < %s, chk path %s, check FO and recovery.",
                chkLargestSst.get(), lastLargestSst.get(), localChkPath);
        }
        return lastFullFiles;
    }

    private void cleanLocalAndRemoteFiles(long chkId, String remotePath, long keepCheckpointNum, File localChkFile) {
        try {
            removeEarlyChk(remotePath, chkId - keepCheckpointNum);
        } catch (IOException ignore) {
            LOGGER.warn("remove Early chk fail and ignore {}, chkId {}, keepChkNum {}", remotePath,
                chkId, keepCheckpointNum);
        }
        Long key;
        while ((key = checkPointFileInfo.lowerKey(chkId)) != null) {
            checkPointFileInfo.remove(key);
        }

        cleanLocalChk(chkId, localChkFile);
    }

    private void removeEarlyChk(String remotePath, long chkId)
        throws IOException {
        final long start = System.currentTimeMillis();
        LOGGER.info("skip remove early chk {} {}", remotePath, chkId);

        FileInfo[] sstFileStatuses = new FileInfo[]{};
        try {
            //if there is no data, the directory will not exist.
            sstFileStatuses = persistIO.listFileInfo(new Path(remotePath, DATAS));
        } catch (Exception e) {
            LOGGER.warn("{} do not have data, just ignore", remotePath);
        }

        Path path = new Path(remotePath);
        PathFilter filter = path1 -> path1.getName().startsWith(META);
        FileInfo[] metaFileStatuses = persistIO.listFileInfo(path, filter);
        Path delMetaPath = getLastMetaFile(chkId, metaFileStatuses);
        if (delMetaPath == null) {
            return;
        }
        Set<String> toBeKepts = getKeptFileName(delMetaPath);
        if (toBeKepts.size() == 0) {
            return;
        }
        // commit tag is the latest file to upload.
        long chkPointTime = persistIO.getFileInfo(new Path(delMetaPath, COMMIT_TAG_FILE)).getModificationTime();
        LOGGER.info("remotePath {}, chkId: {}, chkPointTime {}, toBeKepts: {}",
            remotePath, chkId, new Date(chkPointTime), toBeKepts);

        List<Path> paths = getDelPaths(chkId, chkPointTime, sstFileStatuses, metaFileStatuses, toBeKepts);
        LOGGER.info("RocksDB({}) clean dfs checkpoint: ({}) took {}ms", chkId,
            paths.stream().map(Path::getName).collect(Collectors.joining(",")),
            (System.currentTimeMillis() - start));
        asyncDeletes(paths);
    }

    private List<Path> getDelPaths(long chkId, long chkPointTime, FileInfo[] sstFileStatuses,
                                   FileInfo[] metaFileStatuses, Set<String> toBeKepts) {

        Set<String> toBeDels = new HashSet<>();
        List<Path> paths = Lists.newArrayList();
        for (FileInfo fileStatus : sstFileStatuses) {
            if (fileStatus.getModificationTime() < chkPointTime
                && !toBeKepts.contains(fileStatus.getPath().getName())) {
                toBeDels.add(fileStatus.getPath().getName());
                paths.add(fileStatus.getPath());
                LOGGER.info("delete file: {} time: {}",
                    fileStatus.getPath(), new Date(fileStatus.getModificationTime()));
            }
        }

        LOGGER.info("kepts: {}, dels: {} ", toBeKepts, toBeDels);

        for (final FileInfo fileStatus : metaFileStatuses) {
            long chkVersion = getChkVersion(fileStatus.getPath().getName());
            if (chkVersion < chkId) {
                paths.add(fileStatus.getPath());
            }
        }
        return paths;
    }

    private Path getLastMetaFile(long chkId, FileInfo[] metaFileStatuses) {
        // find the last meta file that indicates the largest committed chkId.
        int maxMetaVersion = 0;
        FileInfo fileInfo = null;
        for (FileInfo fileStatus : metaFileStatuses) {
            int metaVersion = getChkVersion(fileStatus.getPath().getName());
            if (metaVersion < chkId && metaVersion > maxMetaVersion) {
                maxMetaVersion = metaVersion;
                fileInfo = fileStatus;
            }
        }
        if (maxMetaVersion == 0) {
            return null;
        }
        return fileInfo.getPath();
    }

    private Set<String> getKeptFileName(Path metaPath)
        throws IOException {
        Path filesPath = new Path(metaPath, FILES);
        InputStream in = persistIO.open(filesPath);
        String sstString = IOUtils.toString(in, Charset.defaultCharset());
        return Sets.newHashSet(Splitter.on(",").split(sstString));
    }

    private int getChkVersion(String filename) {
        return Integer.parseInt(filename.substring(filename.indexOf('.') + 1));
    }

    private <T> List<T> completeHandler(List<Callable<T>> callers,
                                        ExecutorService executorService) {
        List<Future<T>> futures = new ArrayList<>();
        List<T> results = new ArrayList<>();
        for (final Callable<T> entry : callers) {
            futures.add(executorService.submit(entry));
        }

        try {
            for (Future<T> future : futures) {
                results.add(future.get(persistTimeout, TimeUnit.SECONDS));
            }
        } catch (Exception e) {
            throw new GeaflowRuntimeException(
                RuntimeErrors.INST.stateRocksDbError("persist time out or other exceptions"), e);
        }
        return results;
    }

    private Tuple<Boolean, Long> checkSizeSame(final Path dfsPath, final Path localPath)
        throws IOException {
        long len = persistIO.getFileSize(dfsPath);
        File localFile = new File(localPath.toString());
        return Tuple.of(len == localFile.length(), len);
    }

    private Callable<Long> copyFromLocal(final Path from, final Path to, final long size) {
        return () -> {
            int count = 0;
            int maxTries = 3;
            Tuple<Boolean, Long> checkRes;
            while (true) {
                try {
                    long start = System.currentTimeMillis();
                    persistIO.copyFromLocalFile(from, to);
                    checkRes = checkSizeSame(to, from);
                    if (!checkRes.f0) {
                        LOGGER.warn("upload to dfs size not same {} -> {}", from, to);
                        if (++count == maxTries) {
                            throw new GeaflowRuntimeException(RuntimeErrors.INST.stateRocksDbError("upload to dfs size not same"));
                        }
                    } else {
                        LOGGER.info("upload to dfs size {}KB took {}ms {} -> {}", size / 1024,
                            System.currentTimeMillis() - start, from, to);
                        break;
                    }
                } catch (IOException ex) {
                    if (++count == maxTries) {
                        throw new GeaflowRuntimeException(RuntimeErrors.INST.stateRocksDbError(
                            "upload to dfs exception"), ex);
                    }
                }
            }
            return checkRes.f1;
        };
    }

    private Callable<Long> copyToLocal(final Path from, final Path to) {
        return () -> {
            int count = 0;
            int maxTries = 3;
            Tuple<Boolean, Long> checkRes;
            while (true) {
                try {
                    persistIO.copyToLocalFile(from, to);
                    checkRes = checkSizeSame(from, to);
                    if (!checkRes.f0) {
                        LOGGER.warn("download from dfs size not same {} -> {}", from, to);
                        if (++count == maxTries) {
                            String msg = "download from dfs size not same: " + from;
                            throw new GeaflowRuntimeException(RuntimeErrors.INST.stateRocksDbError(msg));
                        }
                    } else {
                        LOGGER.info("download from dfs {} -> {}", from, to);
                        break;
                    }
                } catch (IOException ex) {
                    if (++count == maxTries) {
                        throw new GeaflowRuntimeException(RuntimeErrors.INST.stateRocksDbError(
                            "copy from dfs exception"), ex);
                    }
                }
            }
            return checkRes.f1;
        };
    }

    private void asyncDeletes(final List<Path> paths) {
        deleteFileService.execute(() -> {
            long start = System.currentTimeMillis();
            for (Path path : paths) {
                try {
                    long s = System.nanoTime();
                    persistIO.delete(path, true);
                    LOGGER.info("async Delete path {} cost {}us", path, (System.nanoTime() - s) / 1000);
                } catch (IOException e) {
                    LOGGER.warn("delete fail", e);
                }
            }
            LOGGER.info("asyncDeletes path {} cost {}ms", paths,
                System.currentTimeMillis() - start);
        });
    }


    private long recoveryData(Path remotePath, File localChkFile,
                              CheckPointFileInfo committedInfo, List<String> list, String remoteMeta)
        throws Exception {
        // fetch data list.
        LOGGER.info("recoveryData {} list {}", remotePath, list);

        List<Callable<Long>> callers = new ArrayList<>();
        for (String sstName : list) {
            callers.add(
                copyToLocal(new Path(Paths.get(remotePath.toString(), DATAS, sstName).toString()),
                    new Path(localChkFile.getAbsolutePath(), sstName)));
        }
        List<String> metaList = persistIO.listFileName(new Path(remoteMeta));
        for (String metaName : metaList) {
            callers.add(
                copyToLocal(new Path(remoteMeta, metaName), new Path(localChkFile.getAbsolutePath(), metaName)));
        }
        long start = System.currentTimeMillis();
        List<Long> res = completeHandler(callers, copyFileService);
        long size = res.stream().mapToLong(i -> i).sum() / 1024;
        long speed = 1000 * size / (System.currentTimeMillis() - start + 1);

        LOGGER.info(
            "RocksDB {} copy ({} to local:{}) lastCommitInfo:{}. size: {}KB, speed: {}KB/s",
            persistIO.getPersistentType(), remotePath, localChkFile, committedInfo, size / 1024, speed);
        String[] localChkFiles = localChkFile.list((dir, name) -> name.endsWith(SST_SUFFIX));
        if (localChkFiles != null) {
            for (String chkFile : localChkFiles) {
                committedInfo.addFullFile(chkFile);
            }
        } else {
            Preconditions.checkArgument(list.size() == 0, "sst is not fetched.");
        }
        return size;
    }

    public static class CheckPointFileInfo {
        private long checkPointId;
        private Set<String> incDataFiles = new HashSet<>();
        private Set<String> fullDataFiles = new HashSet<>();

        public CheckPointFileInfo(long checkPointId) {
            this.checkPointId = checkPointId;
        }

        public long getCheckPointId() {
            return checkPointId;
        }

        public void addIncDataFile(String name) {
            incDataFiles.add(name);
        }

        public void addFullFile(String name) {
            fullDataFiles.add(name);
        }

        public void addFullFiles(Collection<String> name) {
            fullDataFiles.addAll(name);
        }

        @Override
        public String toString() {
            return String
                .format("CheckPointFileInfo [checkPointId=%d, incDataFiles=%s, fullDataFiles=%s]",
                    this.checkPointId, this.incDataFiles, this.fullDataFiles);
        }

        public Set<String> getIncDataFiles() {
            return this.incDataFiles;
        }

        public Set<String> getFullDataFiles() {
            return this.fullDataFiles;
        }
    }
}
