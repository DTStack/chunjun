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

package com.dtstack.chunjun.connector.starrocks.source;

import com.dtstack.chunjun.config.FieldConfig;
import com.dtstack.chunjun.config.SyncConfig;
import com.dtstack.chunjun.connector.starrocks.config.StarRocksConfig;
import com.dtstack.chunjun.connector.starrocks.converter.StarRocksRawTypeMapper;
import com.dtstack.chunjun.connector.starrocks.converter.StarRocksSqlConverter;
import com.dtstack.chunjun.connector.starrocks.converter.StarRocksSyncConverter;
import com.dtstack.chunjun.converter.AbstractRowConverter;
import com.dtstack.chunjun.converter.RawTypeMapper;
import com.dtstack.chunjun.source.SourceFactory;
import com.dtstack.chunjun.util.JsonUtil;
import com.dtstack.chunjun.util.TableUtil;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;

import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;

public class StarRocksSourceFactory extends SourceFactory {

    private final StarRocksConfig starRocksConfig;

    public StarRocksSourceFactory(SyncConfig syncConfig, StreamExecutionEnvironment env) {
        super(syncConfig, env);
        starRocksConfig =
                JsonUtil.toObject(
                        JsonUtil.toJson(syncConfig.getReader().getParameter()),
                        StarRocksConfig.class);
        List<FieldConfig> fieldList = syncConfig.getReader().getFieldList();
        List<String> fieldNameList = new ArrayList<>();
        List<DataType> dataTypeList = new ArrayList<>();
        RawTypeMapper rawTypeMapper = getRawTypeMapper();
        for (FieldConfig fieldConfig : fieldList) {
            if (StringUtils.isBlank(fieldConfig.getValue())) {
                fieldNameList.add(fieldConfig.getName());
                dataTypeList.add(rawTypeMapper.apply(fieldConfig.getType()));
            }
        }

        super.initCommonConf(starRocksConfig);
        starRocksConfig.setFieldNames(fieldNameList.toArray(new String[0]));
        starRocksConfig.setDataTypes(dataTypeList.toArray(new DataType[0]));
    }

    @Override
    public RawTypeMapper getRawTypeMapper() {
        return StarRocksRawTypeMapper::apply;
    }

    @Override
    public DataStream<RowData> createSource() {
        StarRocksInputFormatBuilder inputFormatBuilder =
                new StarRocksInputFormatBuilder(new StarRocksInputFormat());
        inputFormatBuilder.setStarRocksConf(starRocksConfig);
        RowType rowType = TableUtil.createRowType(starRocksConfig.getColumn(), getRawTypeMapper());
        AbstractRowConverter rowConverter;
        if (useAbstractBaseColumn) {
            rowConverter = new StarRocksSyncConverter(rowType, starRocksConfig);
        } else {
            rowConverter = new StarRocksSqlConverter(rowType, null);
        }
        inputFormatBuilder.setRowConverter(rowConverter, useAbstractBaseColumn);
        return createInput(inputFormatBuilder.finish());
    }
}
