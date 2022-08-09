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
package com.dtstack.chunjun.connector.binlog.inputformat;

import com.dtstack.chunjun.connector.binlog.conf.BinlogConf;
import com.dtstack.chunjun.constants.Metrics;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.metrics.MetricGroup;

import com.alibaba.otter.canal.parse.inbound.mysql.MysqlEventParser;
import org.junit.Before;
import org.junit.Test;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.reflect.Whitebox;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.powermock.api.mockito.PowerMockito.mock;
import static org.powermock.api.mockito.PowerMockito.when;

/** @author liuliu 2021/12/30 */
public class UpdrdbBinlogInputFormatTest {
    UpdrdbBinlogInputFormat binlogInputFormat;
    BinlogConf binlogConf;
    InputSplit inputSplit;
    MysqlEventParser mysqlEventParser;

    @Before
    public void setup() {
        binlogInputFormat = new UpdrdbBinlogInputFormat();
        binlogConf = new BinlogConf();
        inputSplit = mock(InputSplit.class);
        mysqlEventParser = mock(MysqlEventParser.class);
    }

    @Test
    public void inputFormatWithUpdrdbSingle() throws Exception {
        Whitebox.setInternalState(binlogInputFormat, "binlogConf", binlogConf);
        Whitebox.setInternalState(binlogConf, "host", "localhost");
        Whitebox.setInternalState(
                binlogConf, "jdbcUrl", "jdbc:mysql://localhost:123/defaultDatabase");
        Whitebox.setInternalState(binlogConf, "isUpdrdb", true);
        Whitebox.setInternalState(binlogConf, "cat", "insert,delete,update");
        Whitebox.setInternalState(binlogConf, "nodeGroupList", Collections.singletonList("DN_001"));
        Whitebox.setInternalState(binlogConf, "username", "liuliu");
        Whitebox.setInternalState(
                binlogConf,
                "lamostTableNameList",
                Arrays.asList("defaultDatabase.testTable1", "defaultDatabase.testTable2"));

        // super
        Map<String, Object> vars = null;
        MetricGroup metricGroup = mock(MetricGroup.class);
        RuntimeContext runtimeContext = mock(RuntimeContext.class);
        when(metricGroup, "getAllVariables").thenReturn(vars);
        when(runtimeContext, "getMetricGroup").thenReturn(metricGroup);
        Whitebox.setInternalState(binlogInputFormat, "runtimeContext", runtimeContext);

        // this
        Whitebox.setInternalState(binlogConf, "filter", "null");
        Whitebox.setInternalState(binlogConf, "table", new ArrayList<>());
        binlogInputFormat.openInputFormat();
        assert binlogConf.username.equals("liuliu@DN_001");

        binlogInputFormat.openInternal(inputSplit);
    }

    @Test
    public void inputFormatWithUpdrdbParallelism() throws Exception {
        Whitebox.setInternalState(binlogInputFormat, "binlogConf", binlogConf);
        Whitebox.setInternalState(binlogConf, "host", "localhost");
        Whitebox.setInternalState(
                binlogConf, "jdbcUrl", "jdbc:mysql://localhost:123/defaultDatabase");
        Whitebox.setInternalState(binlogConf, "isUpdrdb", true);
        Whitebox.setInternalState(binlogConf, "cat", "insert,delete,update");
        Whitebox.setInternalState(
                binlogConf, "nodeGroupList", Arrays.asList("DN_001", "DN_002", "coprocessor"));
        Whitebox.setInternalState(binlogConf, "username", "liuliu");
        Whitebox.setInternalState(
                binlogConf,
                "lamostTableNameList",
                Arrays.asList("defaultDatabase.testTable1", "defaultDatabase.testTable2"));

        // super
        // subtaskIndex
        Map<String, Object> vars = new HashMap<>();
        //        Whitebox.setInternalState(binlogConf,"parallelism",3);
        //        vars.put(Metrics.SUBTASK_INDEX,"1");
        Whitebox.setInternalState(binlogConf, "parallelism", 5);
        vars.put(Metrics.SUBTASK_INDEX, "4");
        MetricGroup metricGroup = PowerMockito.mock(MetricGroup.class);
        RuntimeContext runtimeContext = PowerMockito.mock(RuntimeContext.class);
        PowerMockito.when(metricGroup, "getAllVariables").thenReturn(vars);
        PowerMockito.when(runtimeContext, "getMetricGroup").thenReturn(metricGroup);
        Whitebox.setInternalState(binlogInputFormat, "runtimeContext", runtimeContext);

        // this
        Whitebox.setInternalState(binlogConf, "filter", "null");
        Whitebox.setInternalState(binlogConf, "table", new ArrayList<>());
        binlogInputFormat.openInputFormat();
        System.out.println(binlogConf.username);
        //        assert binlogConf.username.equals("liuliu@DN_002");
        //        assert
        // binlogConf.filter.equals("defaultDatabase.testTable1,defaultDatabase.testTable2");

        binlogInputFormat.openInternal(inputSplit);
    }
}
