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

package com.dtstack.chunjun.connector.hive3.source;

import com.dtstack.chunjun.config.SyncConfig;
import com.dtstack.chunjun.connector.hive3.config.HdfsConfig;
import com.dtstack.chunjun.connector.hive3.converter.HdfsRawTypeMapper;
import com.dtstack.chunjun.connector.hive3.util.Hive3Util;
import com.dtstack.chunjun.converter.AbstractRowConverter;
import com.dtstack.chunjun.converter.RawTypeMapper;
import com.dtstack.chunjun.source.SourceFactory;
import com.dtstack.chunjun.util.GsonUtil;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;

public class Hive3SourceFactory extends SourceFactory {

    HdfsConfig hdfsConfig;

    public Hive3SourceFactory(SyncConfig syncConfig, StreamExecutionEnvironment env) {
        super(syncConfig, env);
        hdfsConfig =
                GsonUtil.GSON.fromJson(
                        GsonUtil.GSON.toJson(syncConfig.getReader().getParameter()),
                        HdfsConfig.class);
        hdfsConfig.setColumn(syncConfig.getReader().getFieldList());
        super.initCommonConf(hdfsConfig);
    }

    @Override
    public RawTypeMapper getRawTypeMapper() {
        return HdfsRawTypeMapper::apply;
    }

    @Override
    public DataStream<RowData> createSource() {
        Hive3InputFormatBuilder builder =
                new Hive3InputFormatBuilder(hdfsConfig.getFileType(), hdfsConfig.isTransaction());
        builder.setHdfsConf(hdfsConfig);
        AbstractRowConverter rowConverter =
                Hive3Util.createRowConverter(
                        useAbstractBaseColumn,
                        hdfsConfig.getFileType(),
                        hdfsConfig.getColumn(),
                        getRawTypeMapper(),
                        hdfsConfig);

        builder.setRowConverter(rowConverter, useAbstractBaseColumn);
        return createInput(builder.finish());
    }
}
