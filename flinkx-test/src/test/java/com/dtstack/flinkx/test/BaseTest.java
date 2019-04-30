package com.dtstack.flinkx.test;

import com.dtstack.flinkx.constants.Metrics;
import org.apache.flink.api.common.JobExecutionResult;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileReader;

public class BaseTest {

    public static Logger LOG = LoggerFactory.getLogger(BaseTest.class);

    private static final String JOB_FILE_PATH = "src/test/resources/unit_test_job";

    protected String readJobContent(String jobName){
        BufferedReader br = null;
        try {
            br = new BufferedReader(new FileReader(JOB_FILE_PATH + jobName + ".json"));
            StringBuilder builder = new StringBuilder();
            while (br.ready()){
                builder.append(br.readLine());
            }

            return builder.toString();
        } catch (Exception e){
            LOG.error("Read job content error");
            throw new RuntimeException(e);
        } finally {
            if (br != null){
                try {
                    br.close();
                } catch (Exception e){
                    LOG.error("Close file reader error:{}", e.getMessage());
                }
            }
        }
    }

    protected void checkResult(JobExecutionResult result, Long expectedDataNumber){
        Long numRead = result.getAccumulatorResult(Metrics.NUM_READS);
        Assert.assertEquals("The read number is wrong",expectedDataNumber,numRead);

        Long numWrite = result.getAccumulatorResult(Metrics.NUM_WRITES);
        Assert.assertEquals("The write number is wrong",expectedDataNumber,numWrite);
    }
}
