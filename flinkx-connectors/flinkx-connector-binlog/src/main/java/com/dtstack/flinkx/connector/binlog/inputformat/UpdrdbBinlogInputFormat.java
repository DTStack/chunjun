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

import com.dtstack.flinkx.connector.binlog.listener.BinlogEventSink;
import com.dtstack.flinkx.connector.binlog.util.BinlogUtil;
import com.dtstack.flinkx.constants.ConstantValue;
import com.dtstack.flinkx.constants.Metrics;
import com.dtstack.flinkx.util.ClassUtil;
import com.dtstack.flinkx.util.DataSyncFactoryUtil;
import com.dtstack.flinkx.util.JsonUtil;

import org.apache.flink.core.io.InputSplit;
import org.apache.flink.table.data.RowData;

import com.alibaba.otter.canal.parse.inbound.mysql.MysqlEventParser;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.LockSupport;

/** @author liuliu 2021/12/30 */
public class UpdrdbBinlogInputFormat extends BinlogInputFormat {
    List<String> groupList = new ArrayList<>();
    private int currentGroupIndex;
    private int groupListSize;
    private List<UpdrdbController> updrdbControllerList;
    private List<BinlogEventSink> updrdbBinlogEventSinkList;

    @Override
    public void openInputFormat() {
        // BaseRichInputFormat
        Map<String, String> vars = getRuntimeContext().getMetricGroup().getAllVariables();
        if (vars != null) {
            jobName = vars.getOrDefault(Metrics.JOB_NAME, "defaultJobName");
            jobId = vars.get(Metrics.JOB_NAME);
            indexOfSubTask = Integer.parseInt(vars.get(Metrics.SUBTASK_INDEX));
        }

        if (useCustomReporter()) {
            customReporter =
                    DataSyncFactoryUtil.discoverMetric(
                            config, getRuntimeContext(), makeTaskFailedWhenReportFailed());
            customReporter.open();
        }

        startTime = System.currentTimeMillis();

        // UpdrdbBinlogInputFormat
        LOG.info(
                "updrdb binlog innodbTableNameList:{}, lamostTableNameList: {}",
                binlogConf.getInnodbTableNameList(),
                binlogConf.getLamostTableNameList());
        ClassUtil.forName(BinlogUtil.DRIVER_NAME, getClass().getClassLoader());

        if (StringUtils.isNotEmpty(binlogConf.getCat())) {
            LOG.info("{}", categories);
            categories =
                    Arrays.asList(
                            binlogConf.getCat().toUpperCase().split(ConstantValue.COMMA_SYMBOL));
        }

        groupList = new ArrayList<>();
        List<String> nodeGroupList = binlogConf.getNodeGroupList();
        int parallelism = binlogConf.getParallelism();
        for (int i = indexOfSubTask; i < nodeGroupList.size(); i += parallelism) {
            groupList.add(nodeGroupList.get(i));
        }
        groupListSize = groupList.size();
        if (groupListSize > 1) {
            // 需要单个slot内开启多个canal连接，List记录所有连接
            updrdbControllerList = new ArrayList<>(groupListSize);
            updrdbBinlogEventSinkList = new ArrayList<>(groupListSize);
            currentGroupIndex = 0;
        } else if (groupListSize == 1) {
            // 一个slot对应单个连接，遵循BinlogInputFormat的机制即可
            String group = groupList.get(0);
            binlogConf.setUsername(
                    BinlogUtil.formatUpdrdbUsername(binlogConf.getUsername(), group));
            if (group.equalsIgnoreCase("coprocessor")) {
                binlogConf.setTable(binlogConf.getInnodbTableNameList());
            } else {
                binlogConf.setTable(binlogConf.getLamostTableNameList());
            }
        }
    }

    @Override
    protected void openInternal(InputSplit inputSplit) {
        if (groupListSize == 0) {
            LOG.info(
                    "updrdb binlog openInternal splitNumber:{},subtaskIndex:{} abort...",
                    inputSplit.getSplitNumber(),
                    indexOfSubTask);
            return;
        }

        LOG.info("binlog openInternal split number:{} start...", inputSplit.getSplitNumber());
        LOG.info("binlog config:{}", JsonUtil.toPrintJson(binlogConf));

        String username = binlogConf.username;
        // 判断是否单个slot内需要启动多个线程
        if (groupListSize > 1) {
            groupList.forEach(
                    group -> {
                        BinlogEventSink binlogEventSink = new BinlogEventSink(this);
                        String filter;
                        // 根据节点类型获取不同的过滤器
                        if (group.equalsIgnoreCase("coprocessor")) {
                            filter =
                                    String.join(
                                            ConstantValue.COMMA_SYMBOL,
                                            binlogConf.getInnodbTableNameList());
                        } else {
                            filter =
                                    String.join(
                                            ConstantValue.COMMA_SYMBOL,
                                            binlogConf.getLamostTableNameList());
                        }
                        String usernameAndGroup = BinlogUtil.formatUpdrdbUsername(username, group);
                        LOG.info(
                                "binlog FilterAfter:{},username: {},subtaskIndex: {}",
                                filter,
                                usernameAndGroup,
                                indexOfSubTask);
                        MysqlEventParser controller =
                                getController(usernameAndGroup, filter, binlogEventSink);
                        updrdbControllerList.add(
                                new UpdrdbController(controller, usernameAndGroup));
                        updrdbBinlogEventSinkList.add(binlogEventSink);
                    });
            updrdbControllerList.forEach(Thread::start);
        } else {
            binlogEventSink = new BinlogEventSink(this);
            String filter = String.join(ConstantValue.COMMA_SYMBOL, binlogConf.getTable());
            LOG.info(
                    "binlog FilterAfter:{},username: {},subtaskIndex: {}",
                    filter,
                    username,
                    indexOfSubTask);
            controller = getController(username, filter, binlogEventSink);
            controller.start();
        }
    }

    @Override
    protected RowData nextRecordInternal(RowData row) {
        if (binlogEventSink != null) {
            return binlogEventSink.takeRowDataFromQueue();
        } else if (groupListSize > 1 && !updrdbControllerList.isEmpty()) {
            // 轮训方式获取获取各个连接的数据
            BinlogEventSink binlogEventSink = updrdbBinlogEventSinkList.get(currentGroupIndex++);
            if (currentGroupIndex >= groupListSize) {
                currentGroupIndex = 0;
            }
            return binlogEventSink.takeRowDataFromQueue();
        }
        LOG.warn("binlog park start");
        LockSupport.park(this);
        LOG.warn("binlog park end...");
        return null;
    }

    @Override
    protected void closeInternal() {
        super.closeInternal();
        if (groupListSize > 0) {
            updrdbControllerList.forEach(
                    updrdbController -> {
                        MysqlEventParser controller = updrdbController.getController();
                        if (controller != null && controller.isStart()) {
                            controller.stop();
                            LOG.info(
                                    "updrdb binlog closeInternal...,username:{} ,entryPosition:{}",
                                    updrdbController.getUsernameAndGroup(),
                                    formatState != null ? formatState.getState() : null);
                        }
                    });
        }
    }
}
