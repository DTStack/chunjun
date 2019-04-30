package com.dtstack.flinkx.test;

import com.dtstack.flinkx.config.DataTransferConfig;
import org.apache.flink.api.common.JobExecutionResult;
import org.junit.Assert;
import org.junit.Test;


public class StreamTest extends BaseTest {

    @Test
    public void readNumTest() throws Exception{
        String job = readJobContent("stream_reade");
        DataTransferConfig config = DataTransferConfig.parse(job);
        Long expectedDataNumber = config.getJob().getContent().get(0).getReader().getParameter().getLongVal("sliceRecordCount", 0);

        JobExecutionResult result = LocalTest.runJob(job, null, null);
        checkResult(result, expectedDataNumber);
    }

    @Test
    public void exceptionIndexTest() {
        String job = readJobContent("stream_exception");
        String message = null;
        try {
            LocalTest.runJob(job, null, null);
        } catch (Exception e){
            message = e.getMessage();
        }

        Assert.assertNotNull("Exception information cannot be empty", message);
    }

}
