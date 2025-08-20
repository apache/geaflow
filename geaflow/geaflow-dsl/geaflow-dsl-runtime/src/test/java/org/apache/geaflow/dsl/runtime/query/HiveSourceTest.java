package org.apache.geaflow.dsl.runtime.query;

import org.apache.geaflow.common.config.keys.FrameworkConfigKeys;
import org.testng.annotations.Test;

public class HiveSourceTest {

    @Test(enabled = false)
    public void testHiveSource_001() throws Exception {
        QueryTester
                .build()
                .withConfig(FrameworkConfigKeys.BATCH_NUMBER_PER_CHECKPOINT.getKey(), 1)
                .withQueryPath("/query/hive_source_001.sql")
                .execute()
                .checkSinkResult();
    }
}
