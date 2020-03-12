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


package com.dtstack.flinkx.test.stream.reader;

import com.dtstack.flinkx.test.annotation.Plugin;
import com.dtstack.flinkx.test.annotation.PluginType;
import com.dtstack.flinkx.test.annotation.TestCase;
import com.dtstack.flinkx.test.core.BaseTest;
import com.dtstack.flinkx.test.core.ReaderUnitTestLauncher;
import com.dtstack.flinkx.test.core.result.ReaderResult;
import com.dtstack.flinkx.util.ExceptionUtil;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 *
 *
 * @author jiangbo
 * @date 2020/2/8
 */
@TestCase(plugin = Plugin.STREAM, type = PluginType.READER)
public class StreamReaderTest extends BaseTest {

    public static final String PLUGIN_NAME = "stream";

    @Override
    protected String getDataSourceName() {
        return PLUGIN_NAME;
    }

    @Override
    protected boolean isDataSourceValid() {
        // stream插件不对接数据源
        return true;
    }

    @Override
    protected void prepareDataInternal() {
        // 不需要准备数据
    }

    @Override
    protected void cleanData() {
        // 不需要清里数据
    }

    @Test
    public void singleChannelNumberTest() {
        try {
            ReaderResult readerResult = (ReaderResult) new ReaderUnitTestLauncher()
                    .withPluginName(PLUGIN_NAME)
                    .withTestCaseName("singleChannelNumberTest")
                    .runJob();

            Assert.assertEquals(50, readerResult.getResultData().size());
        } catch (Exception e) {
            Assert.fail(ExceptionUtil.getErrorMessage(e));
        }
    }

    @Test
    public void multiChannelNumberTest() {
        try {
            ReaderResult readerResult = (ReaderResult) new ReaderUnitTestLauncher()
                    .withPluginName(PLUGIN_NAME)
                    .withTestCaseName("multiChannelNumberTest")
                    .withChannel(2)
                    .runJob();

            Assert.assertEquals(80, readerResult.getResultData().size());
        } catch (Exception e) {
            Assert.fail(ExceptionUtil.getErrorMessage(e));
        }
    }
}
