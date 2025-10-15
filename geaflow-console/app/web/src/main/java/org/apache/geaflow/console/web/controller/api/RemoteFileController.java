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

package org.apache.geaflow.console.web.controller.api;

import javax.servlet.http.HttpServletResponse;
import org.apache.geaflow.console.biz.shared.RemoteFileManager;
import org.apache.geaflow.console.biz.shared.view.RemoteFileView;
import org.apache.geaflow.console.common.dal.model.PageList;
import org.apache.geaflow.console.common.dal.model.RemoteFileSearch;
import org.apache.geaflow.console.web.api.GeaflowApiResponse;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RequestPart;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;


@RestController
@RequestMapping("/remote-files")
public class RemoteFileController {

    @Autowired
    private RemoteFileManager remoteFileManager;

    @GetMapping
    public GeaflowApiResponse<PageList<RemoteFileView>> searchFiles(RemoteFileSearch search) {
        return GeaflowApiResponse.success(remoteFileManager.search(search));
    }

    @GetMapping("/{remoteFileId}")
    public void getFile(HttpServletResponse response, @PathVariable String remoteFileId,
                        @RequestParam(required = false) boolean download) {
        if (download) {
            remoteFileManager.download(remoteFileId, response);

        } else {
            GeaflowApiResponse.success(remoteFileManager.get(remoteFileId)).write(response);
        }
    }

    @PutMapping("/{remoteFileId}")
    public GeaflowApiResponse<Boolean> updateFile(@PathVariable String remoteFileId, @RequestPart MultipartFile file) {
        return GeaflowApiResponse.success(remoteFileManager.upload(remoteFileId, file));
    }

    @DeleteMapping("/{remoteFileId}")
    public GeaflowApiResponse<Boolean> deleteFile(@PathVariable String remoteFileId) {
        return GeaflowApiResponse.success(remoteFileManager.delete(remoteFileId));
    }

}
