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


package com.dtstack.flinkx.test.mysql.reader;

import com.dtstack.flinkx.test.core.ReaderUnitTestLauncher;
import com.dtstack.flinkx.test.core.result.ReaderResult;
import com.dtstack.flinkx.test.mysql.MySqlTest;
import com.dtstack.flinkx.test.rdb.JdbcReaderParameterReplace;
import com.dtstack.flinkx.util.ExceptionUtil;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * @author jiangbo
 * @date 2020/2/8
 */
public class MySqlReaderTest extends MySqlTest {

    @Override
    protected String getPluginType() {
        return "reader";
    }

    /**
     * 全字段单通道读取条数验证
     */
    @Test
    public void singleChannelNumberTest() {
        try {
            ReaderResult readerResult = (ReaderResult) new ReaderUnitTestLauncher()
                    .withPluginName(PLUGIN_NAME)
                    .withParameterReplace(new JdbcReaderParameterReplace(connectionConfig))
                    .withTestCaseName("numberTest")
                    .runJob();

            Assert.assertEquals(readerResult.getResultData().size(), 20);
        } catch (Exception e) {
            Assert.fail(ExceptionUtil.getErrorMessage(e));
        }
    }

    /**
     * 全字段多通道读取条数验证
     */
    @Test
    public void multiChannelNumberTest() {
        try {
            ReaderResult readerResult = (ReaderResult) new ReaderUnitTestLauncher()
                    .withPluginName(PLUGIN_NAME)
                    .withParameterReplace(new JdbcReaderParameterReplace(connectionConfig))
                    .withTestCaseName("numberTest")
                    .withChannel(2)
                    .runJob();

            Assert.assertEquals(readerResult.getResultData().size(), 20);
        } catch (Exception e) {
            Assert.fail(ExceptionUtil.getErrorMessage(e));
        }
    }

    @Test
    public void singleChannelFilterTest() {
        try {
            ReaderResult readerResult = (ReaderResult) new ReaderUnitTestLauncher()
                    .withPluginName(PLUGIN_NAME)
                    .withParameterReplace(new JdbcReaderParameterReplace(connectionConfig))
                    .withTestCaseName("filterTest")
                    .runJob();

            Assert.assertEquals(readerResult.getResultData().size(), 15);
        } catch (Exception e) {
            Assert.fail(ExceptionUtil.getErrorMessage(e));
        }
    }

    @Test
    public void multiChannelFilterTest() {
        try {
            ReaderResult readerResult = (ReaderResult) new ReaderUnitTestLauncher()
                    .withPluginName(PLUGIN_NAME)
                    .withParameterReplace(new JdbcReaderParameterReplace(connectionConfig))
                    .withTestCaseName("filterTest")
                    .withChannel(2)
                    .runJob();

            Assert.assertEquals(readerResult.getResultData().size(), 15);
        } catch (Exception e) {
            Assert.fail(ExceptionUtil.getErrorMessage(e));
        }
    }

    @Test
    public void singleChannelCustomSqlTest() {
        try {
            ReaderResult readerResult = (ReaderResult) new ReaderUnitTestLauncher()
                    .withPluginName(PLUGIN_NAME)
                    .withParameterReplace(new JdbcReaderParameterReplace(connectionConfig))
                    .withTestCaseName("customSqlTest")
                    .runJob();

            Assert.assertEquals(readerResult.getResultData().size(), 17);
        } catch (Exception e) {
            Assert.fail(ExceptionUtil.getErrorMessage(e));
        }
    }

    @Test
    public void multiChannelCustomSqlTest() {
        try {
            ReaderResult readerResult = (ReaderResult) new ReaderUnitTestLauncher()
                    .withPluginName(PLUGIN_NAME)
                    .withParameterReplace(new JdbcReaderParameterReplace(connectionConfig))
                    .withTestCaseName("customSqlTest")
                    .withChannel(2)
                    .runJob();

            Assert.assertEquals(readerResult.getResultData().size(), 17);
        } catch (Exception e) {
            Assert.fail(ExceptionUtil.getErrorMessage(e));
        }
    }

    @Test
    public void singleChannelIncrementWithoutStartLocation() {
        try {
            ReaderResult readerResult = (ReaderResult) new ReaderUnitTestLauncher()
                    .withPluginName(PLUGIN_NAME)
                    .withParameterReplace(new JdbcReaderParameterReplace(connectionConfig))
                    .withTestCaseName("incrementWithoutStartLocation")
                    .runJob();

            Assert.assertEquals(readerResult.getResultData().size(), 20);
        } catch (Exception e) {
            Assert.fail(ExceptionUtil.getErrorMessage(e));
        }
    }

    @Test
    public void multiChannelIncrementWithoutStartLocation() {
        try {
            ReaderResult readerResult = (ReaderResult) new ReaderUnitTestLauncher()
                    .withPluginName(PLUGIN_NAME)
                    .withParameterReplace(new JdbcReaderParameterReplace(connectionConfig))
                    .withTestCaseName("incrementWithoutStartLocation")
                    .withChannel(2)
                    .runJob();

            Assert.assertEquals(readerResult.getResultData().size(), 20);
        } catch (Exception e) {
            Assert.fail(ExceptionUtil.getErrorMessage(e));
        }
    }

    @Test
    public void singleChannelIncrementWithStartLocation() {
        try {
            ReaderResult readerResult = (ReaderResult) new ReaderUnitTestLauncher()
                    .withPluginName(PLUGIN_NAME)
                    .withParameterReplace(new JdbcReaderParameterReplace(connectionConfig))
                    .withTestCaseName("incrementWithStartLocation")
                    .runJob();

            Assert.assertEquals(readerResult.getResultData().size(), 10);
        } catch (Exception e) {
            Assert.fail(ExceptionUtil.getErrorMessage(e));
        }
    }

    @Test
    public void multiChannelIncrementWithStartLocation() {
        try {
            ReaderResult readerResult = (ReaderResult) new ReaderUnitTestLauncher()
                    .withPluginName(PLUGIN_NAME)
                    .withParameterReplace(new JdbcReaderParameterReplace(connectionConfig))
                    .withTestCaseName("incrementWithStartLocation")
                    .withChannel(2)
                    .runJob();

            Assert.assertEquals(readerResult.getResultData().size(), 10);
        } catch (Exception e) {
            Assert.fail(ExceptionUtil.getErrorMessage(e));
        }
    }

    @Test
    public void singleChannelIncrementUseMaxFunc() {
        try {
            ReaderResult readerResult = (ReaderResult) new ReaderUnitTestLauncher()
                    .withPluginName(PLUGIN_NAME)
                    .withParameterReplace(new JdbcReaderParameterReplace(connectionConfig))
                    .withTestCaseName("incrementUseMaxFunc")
                    .runJob();

            Assert.assertEquals(readerResult.getResultData().size(), 19);
        } catch (Exception e) {
            Assert.fail(ExceptionUtil.getErrorMessage(e));
        }
    }

    @Test
    public void multiChannelIncrementUseMaxFunc() {
        try {
            ReaderResult readerResult = (ReaderResult) new ReaderUnitTestLauncher()
                    .withPluginName(PLUGIN_NAME)
                    .withParameterReplace(new JdbcReaderParameterReplace(connectionConfig))
                    .withTestCaseName("incrementUseMaxFunc")
                    .withChannel(2)
                    .runJob();

            Assert.assertEquals(readerResult.getResultData().size(), 19);
        } catch (Exception e) {
            Assert.fail(ExceptionUtil.getErrorMessage(e));
        }
    }
}
