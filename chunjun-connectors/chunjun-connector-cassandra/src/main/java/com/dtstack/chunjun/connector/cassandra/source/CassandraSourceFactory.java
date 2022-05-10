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

package com.dtstack.chunjun.connector.cassandra.source;

import com.dtstack.chunjun.conf.FieldConf;
import com.dtstack.chunjun.conf.SyncConf;
import com.dtstack.chunjun.connector.cassandra.conf.CassandraSourceConf;
import com.dtstack.chunjun.connector.cassandra.converter.CassandraRawTypeConverter;
import com.dtstack.chunjun.connector.cassandra.converter.CassandraRowConverter;
import com.dtstack.chunjun.converter.RawTypeConverter;
import com.dtstack.chunjun.source.SourceFactory;
import com.dtstack.chunjun.util.JsonUtil;
import com.dtstack.chunjun.util.TableUtil;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

import java.util.ArrayList;
import java.util.List;

/**
 * @author tiezhu
 * @since 2021/6/21 星期一
 */
public class CassandraSourceFactory extends SourceFactory {

    private final CassandraSourceConf sourceConf;

    public CassandraSourceFactory(SyncConf syncConf, StreamExecutionEnvironment env) {
        super(syncConf, env);

        sourceConf =
                JsonUtil.toObject(
                        JsonUtil.toJson(syncConf.getReader().getParameter()),
                        CassandraSourceConf.class);
        sourceConf.setColumn(syncConf.getReader().getFieldList());
        super.initFlinkxCommonConf(sourceConf);
    }

    @Override
    public RawTypeConverter getRawTypeConverter() {
        return CassandraRawTypeConverter::apply;
    }

    @Override
    public DataStream<RowData> createSource() {
        CassandraInputFormatBuilder builder = new CassandraInputFormatBuilder();

        builder.setSourceConf(sourceConf);

        List<FieldConf> fieldConfList = sourceConf.getColumn();
        List<String> columnNameList = new ArrayList<>();
        fieldConfList.forEach(fieldConf -> columnNameList.add(fieldConf.getName()));

        final RowType rowType = TableUtil.createRowType(fieldConfList, getRawTypeConverter());
        builder.setRowConverter(new CassandraRowConverter(rowType, columnNameList));

        return createInput(builder.finish());
    }
}
