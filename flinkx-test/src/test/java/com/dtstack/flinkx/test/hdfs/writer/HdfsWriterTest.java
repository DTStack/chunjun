/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


package com.dtstack.flinkx.test.hdfs.writer;

import com.alibaba.fastjson.JSONObject;
import com.dtstack.flinkx.test.core.DefaultParameterReplace;
import com.dtstack.flinkx.test.core.WriterUnitTestLauncher;
import com.dtstack.flinkx.test.core.result.WriterResult;
import com.dtstack.flinkx.test.hdfs.BaseHdfsTest;
import com.dtstack.flinkx.util.ExceptionUtil;
import org.apache.flink.types.Row;
import org.junit.Assert;
import org.testng.annotations.Test;

import java.sql.Timestamp;
import java.util.Date;

/**
 * @author jiangbo
 * @date 2020/2/29
 */
public class HdfsWriterTest extends BaseHdfsTest {

    @Override
    protected void prepareDataInternal() throws Exception {
        // do nothing
    }

    @Test
    public void singleChannelNumberTextFileTest() {
        try {
            int number = 20;
            Row[] records = getTextRecords(number);

            JSONObject parameter = new JSONObject();
            parameter.put("fileName", "fileType=text");
            parameter.put("fileType", "text");
            parameter.put("defaultFS", connectionConfig.getString("fs.default.name"));

            WriterResult result = (WriterResult) new WriterUnitTestLauncher()
                    .withRecords(records)
                    .withPluginName(PLUGIN_NAME)
                    .withParameterReplace(new DefaultParameterReplace(connectionConfig, "writer", "hadoopConfig"))
                    .withParameterReplace(new DefaultParameterReplace(parameter, "writer"))
                    .withTestCaseName("numberTest")
                    .runJob();

            long numberWriter = result.getExecutionResult().getAccumulatorResult("numWrite");
            Assert.assertEquals(number, numberWriter);
        } catch (Exception e) {
            Assert.fail(ExceptionUtil.getErrorMessage(e));
        }
    }

    @Test
    public void multiChannelNumberTextFileTest() {
        try {
            int number = 20;
            Row[] records = getTextRecords(number);

            JSONObject parameter = new JSONObject();
            parameter.put("fileName", "fileType=text");
            parameter.put("fileType", "text");
            parameter.put("defaultFS", connectionConfig.getString("fs.default.name"));

            WriterResult result = (WriterResult) new WriterUnitTestLauncher()
                    .withRecords(records)
                    .withPluginName(PLUGIN_NAME)
                    .withParameterReplace(new DefaultParameterReplace(connectionConfig, "writer", "hadoopConfig"))
                    .withParameterReplace(new DefaultParameterReplace(parameter, "writer"))
                    .withTestCaseName("numberTest")
                    .withChannel(2)
                    .runJob();

            long numberWriter = result.getExecutionResult().getAccumulatorResult("numWrite");
            Assert.assertEquals(number, numberWriter);
        } catch (Exception e) {
            Assert.fail(ExceptionUtil.getErrorMessage(e));
        }
    }

    private Row[] getTextRecords(int number){
        Row[] records = new Row[number];
        for (int i = 0; i < number; i++) {
            Row record = new Row(3);
            record.setField(0, i);
            record.setField(0, (short)i);
            record.setField(0, i);
            record.setField(0, (long)i);
            record.setField(0, 2.34);
            record.setField(0, 4.342);
            record.setField(0, 45.234);
            record.setField(0, "string_" + i);
            record.setField(0, "varchar_" + i);
            record.setField(0, "char_" + i);
            record.setField(0, true);
            record.setField(0, new Date());
            record.setField(0, new Timestamp(System.currentTimeMillis()));
            record.setField(0, new Byte[3]);

            records[i] = record;
        }

        return records;
    }
}
