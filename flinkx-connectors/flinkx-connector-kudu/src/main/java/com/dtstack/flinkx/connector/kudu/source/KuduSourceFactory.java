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

package com.dtstack.flinkx.connector.kudu.source;

import com.dtstack.flinkx.conf.FieldConf;
import com.dtstack.flinkx.conf.SyncConf;
import com.dtstack.flinkx.connector.kudu.conf.KuduSourceConf;
import com.dtstack.flinkx.connector.kudu.converter.KuduRawTypeConverter;
import com.dtstack.flinkx.connector.kudu.converter.KuduRowConverter;
import com.dtstack.flinkx.converter.RawTypeConverter;
import com.dtstack.flinkx.source.SourceFactory;
import com.dtstack.flinkx.util.JsonUtil;
import com.dtstack.flinkx.util.TableUtil;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

import java.util.ArrayList;
import java.util.List;

/**
 * @author tiezhu
 * @since 2021/6/9 星期三
 */
public class KuduSourceFactory extends SourceFactory {

    private final KuduSourceConf sourceConf;

    public KuduSourceFactory(SyncConf syncConf, StreamExecutionEnvironment env) {
        super(syncConf, env);

        sourceConf =
                JsonUtil.toObject(
                        JsonUtil.toJson(syncConf.getReader().getParameter()), KuduSourceConf.class);
        sourceConf.setColumn(syncConf.getReader().getFieldList());
        sourceConf.setKerberos(sourceConf.conventHadoopConfig());
        super.initFlinkxCommonConf(sourceConf);
    }

    @Override
    public RawTypeConverter getRawTypeConverter() {
        return KuduRawTypeConverter::apply;
    }

    @Override
    public DataStream<RowData> createSource() {
        KuduInputFormatBuilder builder = new KuduInputFormatBuilder();

        builder.setKuduSourceConf(sourceConf);

        final RowType rowType =
                TableUtil.createRowType(sourceConf.getColumn(), getRawTypeConverter());

        List<FieldConf> fieldConfList = sourceConf.getColumn();
        List<String> columnNameList = new ArrayList<>();
        fieldConfList.forEach(fieldConf -> columnNameList.add(fieldConf.getName()));

        builder.setRowConverter(new KuduRowConverter(rowType, columnNameList));

        return createInput(builder.finish());
    }
}
