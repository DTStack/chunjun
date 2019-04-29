package com.dtstack.flinkx.test;

import org.apache.flink.api.common.JobExecutionResult;
import org.junit.*;


public class MySqlTest extends BaseTest {

    @Test
    public void readTest() throws Exception{
        String job = readJobContent("");
        Long expectedDataNumber = 100L;

        JobExecutionResult result = LocalTest.runJob(job, null, null);
        checkResult(result, expectedDataNumber);
    }

}
