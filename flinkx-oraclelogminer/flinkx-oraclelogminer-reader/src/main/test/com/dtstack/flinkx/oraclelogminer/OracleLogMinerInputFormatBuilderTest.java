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

package com.dtstack.flinkx.oraclelogminer;

import com.dtstack.flinkx.oraclelogminer.format.LogMinerConnection;
import com.dtstack.flinkx.oraclelogminer.format.OracleInfo;
import com.dtstack.flinkx.oraclelogminer.reader.OracleLogMinerInputFormatBuilder;
import com.dtstack.flinkx.util.RetryUtil;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;

import java.sql.DriverManager;
import java.sql.SQLException;

@RunWith(PowerMockRunner.class)
@PrepareForTest({DriverManager.class, LogMinerConnection.class, RetryUtil.class})
public class OracleLogMinerInputFormatBuilderTest {


    @Before
    public void setup() throws SQLException {

    }

    @Test
    public void testCheckOracleInfo() throws Exception {
        OracleLogMinerInputFormatBuilder oracleLogMinerInputFormatBuilder = new OracleLogMinerInputFormatBuilder();
        OracleInfo oracleInfo = new OracleInfo();
        oracleInfo.setVersion(9);
        oracleInfo.setRacMode(true);
        oracleInfo.setCdbMode(true);
        oracleInfo.setEncoding("GBK");
        StringBuilder stringBuilder = new StringBuilder(128);
        Whitebox.<Void>invokeMethod(oracleLogMinerInputFormatBuilder, "checkOracleInfo", oracleInfo, stringBuilder);
        Assert.assertTrue(stringBuilder.length() > 0);
    }

    @Test
    public void testCheckOracleInfo1() throws Exception {
        OracleLogMinerInputFormatBuilder oracleLogMinerInputFormatBuilder = new OracleLogMinerInputFormatBuilder();
        OracleInfo oracleInfo = new OracleInfo();
        oracleInfo.setVersion(10);
        oracleInfo.setRacMode(true);
        oracleInfo.setCdbMode(true);
        oracleInfo.setEncoding("GBK");
        StringBuilder stringBuilder = new StringBuilder(128);
        Whitebox.<Void>invokeMethod(oracleLogMinerInputFormatBuilder, "checkOracleInfo", oracleInfo, stringBuilder);
        Assert.assertEquals(0, stringBuilder.length());
    }

    @Test
    public void testCheckTableFormat() throws Exception {
        OracleLogMinerInputFormatBuilder oracleLogMinerInputFormatBuilder = new OracleLogMinerInputFormatBuilder();
        StringBuilder stringBuilder = new StringBuilder(128);
        Whitebox.<Void>invokeMethod(oracleLogMinerInputFormatBuilder, "checkTableFormat", stringBuilder, "a.b,a.c", true);
        Assert.assertEquals(0, stringBuilder.length());
    }

    @Test
    public void testCheckTableFormat1() throws Exception {
        OracleLogMinerInputFormatBuilder oracleLogMinerInputFormatBuilder = new OracleLogMinerInputFormatBuilder();
        StringBuilder stringBuilder = new StringBuilder(128);
        Whitebox.<Void>invokeMethod(oracleLogMinerInputFormatBuilder, "checkTableFormat", stringBuilder, "a.a.b,a.c", true);
        Assert.assertEquals(0, stringBuilder.length());
    }

    @Test
    public void testCheckTableFormat2() throws Exception {
        OracleLogMinerInputFormatBuilder oracleLogMinerInputFormatBuilder = new OracleLogMinerInputFormatBuilder();
        StringBuilder stringBuilder = new StringBuilder(128);
        Whitebox.<Void>invokeMethod(oracleLogMinerInputFormatBuilder, "checkTableFormat", stringBuilder, "a.a.a.b,a.c", true);
        Assert.assertTrue(stringBuilder.length() > 0);
    }

}
