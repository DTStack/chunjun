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


package com.dtstack.flinkx.test.stream.writer;

import com.dtstack.flinkx.test.annotation.Plugin;
import com.dtstack.flinkx.test.annotation.PluginType;
import com.dtstack.flinkx.test.annotation.TestCase;
import com.dtstack.flinkx.test.core.BaseTest;
import com.dtstack.flinkx.test.core.WriterUnitTestLauncher;
import com.dtstack.flinkx.test.core.result.WriterResult;
import com.dtstack.flinkx.util.ExceptionUtil;
import org.apache.flink.types.Row;
import org.junit.Assert;
import org.testng.annotations.Test;

/**
 * @author jiangbo
 * @date 2020/2/20
 */
@TestCase(plugin = Plugin.STREAM, type = PluginType.WRITER)
public class StreamWriterTest extends BaseTest {

    public static final String PLUGIN_NAME = "stream";

    @Override
    protected String getDataSourceName() {
        return PLUGIN_NAME;
    }

    @Override
    protected boolean isDataSourceValid() {
        return true;
    }

    @Override
    protected void prepareDataInternal() throws Exception {

    }

    @Override
    protected void cleanData() throws Exception {

    }

    @Test
    public void singleChannelNumberTest() {
        try {
            Row[] records = new Row[20];
            for (int i = 0; i < 20; i++) {
                Row record = new Row(3);
                record.setField(0, "col1");
                record.setField(1, "col2");
                record.setField(2, "col3");

                records[i] = record;
            }

            WriterResult result = (WriterResult) new WriterUnitTestLauncher()
                    .withRecords(records)
                    .withPluginName(PLUGIN_NAME)
                    .withTestCaseName("singleChannelNumberTest")
                    .runJob();

            long numberWriter = result.getExecutionResult().getAccumulatorResult("numWrite");
            Assert.assertEquals(20, numberWriter);
        } catch (Exception e) {
            Assert.fail(ExceptionUtil.getErrorMessage(e));
        }
    }
}
