package com.dtstack.flinkx.pgwal.reader;

import com.dtstack.flinkx.config.DataTransferConfig;
import com.google.common.collect.Lists;
import junit.framework.TestCase;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.Assert;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PgwalReaderTest extends TestCase {

    private PgwalReader reader;

    public void setUp() throws Exception {
        super.setUp();
        StreamExecutionEnvironment environment = new LocalStreamEnvironment();
        Map<String, Object> params = new HashMap<>();
        Map<String, Object> jobParams = new HashMap<>();
        jobParams.put("setting", new HashMap<>());
        Map<String, Object> job = new HashMap<>(jobParams);
        List<Map<String, Object>> contents = new ArrayList<>();
        Map<String, Object> content = new HashMap<>();
        Map<String, Object> readParams = new HashMap<>();

        content.put("reader", readParams);
        readParams.put("parameter", readParams);
        readParams.put("username", "dummy");
        readParams.put("password", "dummy");
        readParams.put("url", "dummy");
        readParams.put("databaseName", "dummy");
        readParams.put("cat", "dummy");
        readParams.put("tableList", Lists.newArrayList("a"));

        Map<String, Object> writerParams = new HashMap<>();
        content.put("writer", writerParams);
        contents.add(content);
        job.put("content", contents);
        params.put("job", job);
        DataTransferConfig config = new DataTransferConfig(params);
        reader = new PgwalReader(config, environment);
    }

    public void tearDown() throws Exception {
        reader = null;
    }

    public void testReadData() {
        try {
            Assert.assertNotNull(reader.readData());
        }catch (Throwable e) {
            fail(e.getMessage());
        }
    }
}