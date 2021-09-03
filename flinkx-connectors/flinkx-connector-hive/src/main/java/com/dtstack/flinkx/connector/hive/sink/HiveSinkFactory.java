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
package com.dtstack.flinkx.connector.hive.sink;

import com.dtstack.flinkx.conf.SyncConf;
import com.dtstack.flinkx.connector.hdfs.converter.HdfsRawTypeConverter;
import com.dtstack.flinkx.connector.hive.conf.HiveConf;
import com.dtstack.flinkx.connector.hive.util.HiveUtil;
import com.dtstack.flinkx.converter.RawTypeConverter;
import com.dtstack.flinkx.sink.SinkFactory;
import com.dtstack.flinkx.util.GsonUtil;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.table.data.RowData;

import org.apache.commons.lang3.StringUtils;

/**
 * Date: 2021/06/22 Company: www.dtstack.com
 *
 * @author tudou
 */
public class HiveSinkFactory extends SinkFactory {

    private final HiveConf hiveConf;

    public HiveSinkFactory(SyncConf config) {
        super(config);
        hiveConf =
                GsonUtil.GSON.fromJson(
                        GsonUtil.GSON.toJson(config.getWriter().getParameter()), HiveConf.class);
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
        super.initFlinkxCommonConf(hiveConf);
    }

    @Override
    public DataStreamSink<RowData> createSink(DataStream<RowData> dataSet) {
        HiveOutputFormatBuilder builder = new HiveOutputFormatBuilder();
        builder.setHiveConf(hiveConf);
        return createOutput(dataSet, builder.finish());
    }

    @Override
    public RawTypeConverter getRawTypeConverter() {
        return HdfsRawTypeConverter::apply;
    }
}
