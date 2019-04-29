package com.dtstack.flinkx.test;

import com.dtstack.flinkx.constants.Metrics;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.accumulators.LongCounter;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

public class BaseTest {

    public static Logger LOG = LoggerFactory.getLogger(BaseTest.class);

    protected String readJobContent(String jobPath){
        File file = new File(jobPath);



        return null;
    }

    protected void checkResult(JobExecutionResult result, Long expectedDataNumber){
        LongCounter numReadCounter = result.getAccumulatorResult(Metrics.NUM_READS);
        Assert.assertEquals("The read number is wrong",expectedDataNumber,numReadCounter.getLocalValue());

        LongCounter numWriteCounter = result.getAccumulatorResult(Metrics.NUM_WRITES);
        Assert.assertEquals("The write number is wrong",expectedDataNumber,numWriteCounter.getLocalValue());
    }
}
