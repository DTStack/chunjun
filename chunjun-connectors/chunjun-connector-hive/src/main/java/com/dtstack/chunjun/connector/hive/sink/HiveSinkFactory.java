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
package com.dtstack.chunjun.connector.hive.sink;

import com.dtstack.chunjun.config.SyncConfig;
import com.dtstack.chunjun.connector.hdfs.converter.HdfsRawTypeMapper;
import com.dtstack.chunjun.connector.hive.config.HiveConfig;
import com.dtstack.chunjun.connector.hive.util.HiveUtil;
import com.dtstack.chunjun.converter.RawTypeMapper;
import com.dtstack.chunjun.sink.SinkFactory;
import com.dtstack.chunjun.util.GsonUtil;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.table.data.RowData;

import org.apache.commons.lang3.StringUtils;

public class HiveSinkFactory extends SinkFactory {

    private final HiveConfig hiveConf;

    public HiveSinkFactory(SyncConfig config) {
        super(config);
        hiveConf =
                GsonUtil.GSON.fromJson(
                        GsonUtil.GSON.toJson(config.getWriter().getParameter()), HiveConfig.class);
        hiveConf.setColumn(config.getWriter().getFieldList());
        hiveConf.setDistributeTableMapping(
                HiveUtil.formatHiveDistributeInfo(hiveConf.getDistributeTable()));
        hiveConf.setTableInfos(
                HiveUtil.formatHiveTableInfo(
                        hiveConf.getTablesColumn(),
                        hiveConf.getPartition(),
                        hiveConf.getFieldDelimiter(),
                        hiveConf.getFileType()));
        if (StringUtils.isBlank(hiveConf.getAnalyticalRules())) {
            hiveConf.setTableName(
                    hiveConf.getTableInfos()
                            .entrySet()
                            .iterator()
                            .next()
                            .getValue()
                            .getTableName());
        } else {
            hiveConf.setTableName(hiveConf.getAnalyticalRules());
            hiveConf.setAutoCreateTable(true);
        }
        super.initCommonConf(hiveConf);
    }

    @Override
    public DataStreamSink<RowData> createSink(DataStream<RowData> dataSet) {
        HiveOutputFormatBuilder builder = new HiveOutputFormatBuilder();
        builder.setHiveConf(hiveConf);
        return createOutput(dataSet, builder.finish());
    }

    @Override
    public RawTypeMapper getRawTypeMapper() {
        return HdfsRawTypeMapper::apply;
    }
}
