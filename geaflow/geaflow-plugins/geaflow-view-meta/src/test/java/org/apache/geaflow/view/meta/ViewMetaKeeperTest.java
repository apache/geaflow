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

package org.apache.geaflow.view.meta;

import java.io.File;
import org.apache.geaflow.common.config.Configuration;
import org.apache.geaflow.file.FileConfigKeys;
import org.apache.geaflow.view.meta.ViewMetaBookKeeper.ViewMetaKeeper;
import org.testng.Assert;
import org.testng.annotations.Test;

public class ViewMetaKeeperTest {

    @Test
    public void testFO() throws Exception {
        Configuration configuration = new Configuration();
        configuration.put(FileConfigKeys.PERSISTENT_TYPE, "LOCAL");
        configuration.put(FileConfigKeys.ROOT, "/tmp/");

        testFO("panguInfo" + System.currentTimeMillis(), configuration);
    }

    private void testFO(String jobName, Configuration config) throws Exception {
        ViewMetaKeeper keeper = new ViewMetaKeeper();
        keeper.init(jobName, config);

        keeper.save("successBatchId", "10".getBytes());
        Assert.assertEquals(keeper.get("successBatchId"), "10".getBytes());
        keeper.archive();

        ViewMetaKeeper sharedKeeper = new ViewMetaKeeper();
        sharedKeeper.init("shared", config);
        Thread.sleep(1000);
        Assert.assertEquals(sharedKeeper.get(jobName, "successBatchId"), "10".getBytes());

        keeper.save("done", "true".getBytes());
        keeper.archive();

        Thread.sleep(1000);
        Assert.assertEquals(sharedKeeper.get(jobName, "done"), "true".getBytes());

        // normal fo
        keeper = new ViewMetaKeeper();
        keeper.init(jobName, config);
        Assert.assertEquals(keeper.get("successBatchId"), "10".getBytes());
        Assert.assertEquals(keeper.get("done"), "true".getBytes());

        String filePath = "/tmp/" + jobName + "/view.meta";
        Assert.assertTrue(new File(filePath).exists());

        // fail fo, luckily, we have backup file
        new File(filePath).renameTo(new File(filePath + ".bak"));
        Thread.sleep(1000);
        keeper = new ViewMetaKeeper();
        keeper.init(jobName, config);
        Assert.assertEquals(keeper.get("successBatchId"), "10".getBytes());
        Assert.assertEquals(keeper.get("done"), "true".getBytes());
        Assert.assertEquals(sharedKeeper.get(jobName, "done"), "true".getBytes());

        keeper.save("finish", "true".getBytes());
        keeper.archive();

        keeper = new ViewMetaKeeper();
        keeper.init(jobName, config);
        Assert.assertEquals(keeper.get("successBatchId"), "10".getBytes());
        Assert.assertEquals(keeper.get("done"), "true".getBytes());
        Assert.assertEquals(keeper.get("finish"), "true".getBytes());
        Thread.sleep(1000);
        Assert.assertEquals(sharedKeeper.get(jobName, "finish"), "true".getBytes());
        keeper.archive();

        Assert.assertTrue(new File(filePath).exists());
        Assert.assertFalse(new File(filePath + ".bak").exists());
    }
}
