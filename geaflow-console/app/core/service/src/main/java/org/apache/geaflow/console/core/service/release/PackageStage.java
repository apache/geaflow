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

package org.apache.geaflow.console.core.service.release;

import com.alibaba.fastjson.JSON;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.geaflow.console.common.util.Md5Util;
import org.apache.geaflow.console.common.util.ZipUtil;
import org.apache.geaflow.console.common.util.ZipUtil.GeaflowZipEntry;
import org.apache.geaflow.console.common.util.ZipUtil.MemoryZipEntry;
import org.apache.geaflow.console.common.util.exception.GeaflowException;
import org.apache.geaflow.console.core.model.release.GeaflowRelease;
import org.apache.geaflow.console.core.model.release.JobPlanBuilder;
import org.apache.geaflow.console.core.model.release.ReleaseUpdate;
import org.apache.geaflow.console.core.service.file.RemoteFileStorage;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class PackageStage extends GeaflowBuildStage {

    private static final String GQL_FILE_NAME = "user.gql";
    private static final String CONF_FILE_NAME = "user.conf";

    @Autowired
    private RemoteFileStorage remoteFileStorage;

    @Override
    public void init(GeaflowRelease release) {
        try {
            List<GeaflowZipEntry> zipEntries;
            if (release.getJob().isApiJob()) {
                MemoryZipEntry entry = new MemoryZipEntry("empty.txt", "");
                zipEntries = Collections.singletonList(entry);

            } else {
                String code = release.getJob().getUserCode().getText();
                Map<String, Integer> parallelismMap = JobPlanBuilder.getParallelismMap(release.getJobPlan());
                MemoryZipEntry codeEntry = new MemoryZipEntry(GQL_FILE_NAME, code);
                MemoryZipEntry confEntry = new MemoryZipEntry(CONF_FILE_NAME, JSON.toJSONString(parallelismMap));
                zipEntries = Arrays.asList(codeEntry, confEntry);
            }
            // url and md5
            String path = RemoteFileStorage.getPackageFilePath(release.getJob().getId(), release.getReleaseVersion());
            release.setUrl(remoteFileStorage.upload(path, ZipUtil.buildZipInputStream(zipEntries)));
            release.setMd5(Md5Util.encodeFile(ZipUtil.buildZipInputStream(zipEntries)));
        } catch (IOException e) {
            throw new GeaflowException("Package job {} fail ", release.getJob().getName(), e);
        }
    }

    @Override
    public boolean update(GeaflowRelease release, ReleaseUpdate update) {
        return false;
    }
}
