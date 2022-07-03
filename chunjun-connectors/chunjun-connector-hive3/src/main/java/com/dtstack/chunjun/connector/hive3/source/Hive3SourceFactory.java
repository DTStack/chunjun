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

import com.dtstack.chunjun.conf.SyncConf;
import com.dtstack.chunjun.connector.hive3.conf.HdfsConf;
import com.dtstack.chunjun.connector.hive3.converter.HdfsRawTypeConverter;
import com.dtstack.chunjun.connector.hive3.util.Hive3Util;
import com.dtstack.chunjun.converter.AbstractRowConverter;
import com.dtstack.chunjun.converter.RawTypeConverter;
import com.dtstack.chunjun.source.SourceFactory;
import com.dtstack.chunjun.util.GsonUtil;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;

/** @author liuliu 2022/3/23 */
public class Hive3SourceFactory extends SourceFactory {
    HdfsConf hdfsConf;

    public Hive3SourceFactory(SyncConf syncConf, StreamExecutionEnvironment env) {
        super(syncConf, env);
        hdfsConf =
                GsonUtil.GSON.fromJson(
                        GsonUtil.GSON.toJson(syncConf.getReader().getParameter()), HdfsConf.class);
        hdfsConf.setColumn(syncConf.getReader().getFieldList());
        super.initCommonConf(hdfsConf);
    }

    @Override
    public RawTypeConverter getRawTypeConverter() {
        return HdfsRawTypeConverter::apply;
    }

    @Override
    public DataStream<RowData> createSource() {
        Hive3InputFormatBuilder builder =
                new Hive3InputFormatBuilder(hdfsConf.getFileType(), hdfsConf.isTransaction());
        builder.setHdfsConf(hdfsConf);
        AbstractRowConverter rowConverter =
                Hive3Util.createRowConverter(
                        useAbstractBaseColumn,
                        hdfsConf.getFileType(),
                        hdfsConf.getColumn(),
                        getRawTypeConverter(),
                        hdfsConf);

        builder.setRowConverter(rowConverter, useAbstractBaseColumn);
        return createInput(builder.finish());
    }
}
