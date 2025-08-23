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

package org.apache.geaflow.console.core.model.file;

import com.google.common.base.Preconditions;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.apache.commons.lang3.StringUtils;
import org.apache.geaflow.console.common.util.type.GeaflowFileType;
import org.apache.geaflow.console.core.model.GeaflowName;

@Getter
@Setter
@NoArgsConstructor
public class GeaflowRemoteFile extends GeaflowName {

    protected GeaflowFileType type;

    protected String path;

    protected String url;

    protected String md5;

    public GeaflowRemoteFile(GeaflowFileType type) {
        this.type = type;
    }

    public static boolean md5Equals(GeaflowRemoteFile left, GeaflowRemoteFile right) {
        if (left == null && right == null) {
            return true;
        }

        if (left != null && right != null) {
            return StringUtils.equals(left.md5, right.md5);
        }

        return false;
    }

    @Override
    public void validate() {
        super.validate();
        Preconditions.checkNotNull(type, "Invalid type");
        Preconditions.checkNotNull(path, "Invalid path");
        Preconditions.checkNotNull(url, "Invalid url");
        Preconditions.checkNotNull(md5, "Invalid md5");
    }

}
