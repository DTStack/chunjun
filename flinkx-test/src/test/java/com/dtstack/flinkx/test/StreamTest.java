package com.dtstack.flinkx.test;

import org.apache.flink.api.common.JobExecutionResult;
import org.junit.Assert;
import org.junit.Test;

public class StreamTest extends BaseTest {

    @Test
    public void readNumTest() throws Exception{
        String job = readJobContent("");
        Long expectedDataNumber = 100L;

        JobExecutionResult result = LocalTest.runJob(job, null, null);
        checkResult(result, expectedDataNumber);
    }

    @Test
    public void exceptionIndexTest() {
        String job = readJobContent("");
        String message = null;
        try {
            LocalTest.runJob(job, null, null);
        } catch (Exception e){
            message = e.getMessage();
        }

        Assert.assertNotNull("", message);
    }
}
