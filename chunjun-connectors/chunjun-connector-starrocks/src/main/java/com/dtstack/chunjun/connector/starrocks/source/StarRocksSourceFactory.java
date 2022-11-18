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

import com.dtstack.chunjun.config.FieldConf;
import com.dtstack.chunjun.config.SyncConf;
import com.dtstack.chunjun.connector.starrocks.conf.StarRocksConf;
import com.dtstack.chunjun.connector.starrocks.converter.StarRocksColumnConverter;
import com.dtstack.chunjun.connector.starrocks.converter.StarRocksRawTypeConverter;
import com.dtstack.chunjun.connector.starrocks.converter.StarRocksRowConverter;
import com.dtstack.chunjun.converter.AbstractRowConverter;
import com.dtstack.chunjun.converter.RawTypeConverter;
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

/** @author liuliu 2022/7/27 */
public class StarRocksSourceFactory extends SourceFactory {

    private final StarRocksConf starRocksConf;

    public StarRocksSourceFactory(SyncConf syncConf, StreamExecutionEnvironment env) {
        super(syncConf, env);
        starRocksConf =
                JsonUtil.toObject(
                        JsonUtil.toJson(syncConf.getReader().getParameter()), StarRocksConf.class);
        List<FieldConf> fieldList = syncConf.getReader().getFieldList();
        List<String> fieldNameList = new ArrayList<>();
        List<DataType> dataTypeList = new ArrayList<>();
        RawTypeConverter rawTypeConverter = getRawTypeConverter();
        for (FieldConf fieldConf : fieldList) {
            if (StringUtils.isBlank(fieldConf.getValue())) {
                fieldNameList.add(fieldConf.getName());
                dataTypeList.add(rawTypeConverter.apply(fieldConf.getType()));
            }
        }

        super.initCommonConf(starRocksConf);
        starRocksConf.setFieldNames(fieldNameList.toArray(new String[0]));
        starRocksConf.setDataTypes(dataTypeList.toArray(new DataType[0]));
    }

    @Override
    public RawTypeConverter getRawTypeConverter() {
        return StarRocksRawTypeConverter::apply;
    }

    @Override
    public DataStream<RowData> createSource() {
        StarRocksInputFormatBuilder inputFormatBuilder =
                new StarRocksInputFormatBuilder(new StarRocksInputFormat());
        inputFormatBuilder.setStarRocksConf(starRocksConf);
        RowType rowType = TableUtil.createRowType(starRocksConf.getColumn(), getRawTypeConverter());
        AbstractRowConverter rowConverter;
        if (useAbstractBaseColumn) {
            rowConverter = new StarRocksColumnConverter(rowType, starRocksConf);
        } else {
            rowConverter = new StarRocksRowConverter(rowType, null);
        }
        inputFormatBuilder.setRowConverter(rowConverter, useAbstractBaseColumn);
        return createInput(inputFormatBuilder.finish());
    }
}
