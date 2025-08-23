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

package org.apache.geaflow.file.dfs;

import java.io.File;
import java.util.List;
import org.apache.commons.io.FileUtils;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.file.FileConfigKeys;
import org.apache.geaflow.file.FileInfo;
import org.apache.geaflow.file.IPersistentIO;
import org.apache.geaflow.file.PersistentIOBuilder;
import org.apache.hadoop.fs.Path;
import org.testng.Assert;
import org.testng.annotations.Test;

public class LocalIOTest {

    @Test
    public void test() throws Exception {
        FileUtils.touch(new File("/tmp/README"));

        Configuration configuration = new Configuration();
        configuration.put(FileConfigKeys.PERSISTENT_TYPE, "LOCAL");
        IPersistentIO persistentIO = PersistentIOBuilder.build(configuration);
        persistentIO.delete(new Path("/tmp/geaflow/chk/myName2"), true);

        for (int i = 0; i < 101; i++) {
            persistentIO.copyFromLocalFile(new Path("/tmp/README"),
                new Path("/tmp/geaflow/chk/myName/datas/README" + i));
        }
        persistentIO.copyFromLocalFile(new Path("/tmp/README"), new Path("/tmp/geaflow/chk/myName/0/README"));
        persistentIO.copyFromLocalFile(new Path("/tmp/README"), new Path("/tmp/geaflow/chk/myName/1/README"));

        persistentIO.renameFile(new Path("/tmp/geaflow/chk/myName/"), new Path("/tmp/geaflow/chk/myName2"));
        List<String> list = persistentIO.listFileName(new Path("/tmp/geaflow/chk/myName2"));
        Assert.assertEquals(list.size(), 3);

        FileInfo[] res = persistentIO.listFileInfo(new Path("/tmp/geaflow/chk/myName2/datas"));
        Assert.assertEquals(res.length, 101);

        persistentIO.renameFile(new Path("/tmp/geaflow/chk/myName2/datas/README46"),
            new Path("/tmp/geaflow/chk/myName2/datas/MYREADME46"));
        Assert.assertTrue(persistentIO.exists(new Path("/tmp/geaflow/chk/myName2/datas/MYREADME46")));
        Assert.assertFalse(persistentIO.exists(new Path("/tmp/geaflow/chk/myName2/datas/README46")));

        persistentIO.delete(new Path("/tmp/geaflow/chk/myName2"), true);
    }
}
