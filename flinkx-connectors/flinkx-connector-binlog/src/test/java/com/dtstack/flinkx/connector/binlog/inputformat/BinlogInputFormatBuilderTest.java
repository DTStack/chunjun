/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.dtstack.flinkx.connector.binlog.inputformat;

import com.dtstack.flinkx.conf.FlinkxCommonConf;
import com.dtstack.flinkx.connector.binlog.conf.BinlogConf;

import org.junit.Before;
import org.junit.Test;
import org.powermock.reflect.Whitebox;

import static org.powermock.api.mockito.PowerMockito.mock;
import static org.powermock.api.mockito.PowerMockito.when;

/** @author liuliu 2021/12/30 */
public class BinlogInputFormatBuilderTest {

    BinlogInputFormatBuilder builder;
    BinlogInputFormat inputFormat;

    @Before
    public void setup() {
        builder = mock(BinlogInputFormatBuilder.class);
        inputFormat = new BinlogInputFormat();
    }

    @Test
    public void finishTest() {
        FlinkxCommonConf commonConf = new FlinkxCommonConf();
        commonConf.setCheckFormat(true);
        BinlogConf binlogConf = new BinlogConf();
        binlogConf.setUpdrdb(true);
        Whitebox.setInternalState(inputFormat, "config", commonConf);
        Whitebox.setInternalState(inputFormat, "binlogConf", binlogConf);
        Whitebox.setInternalState(builder, "format", inputFormat);
        when(builder.finish()).thenCallRealMethod();
        assert builder.finish() instanceof UpdrdbBinlogInputFormat;
    }
}
