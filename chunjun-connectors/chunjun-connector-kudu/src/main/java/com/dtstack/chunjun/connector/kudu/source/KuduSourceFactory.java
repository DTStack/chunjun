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

package com.dtstack.chunjun.connector.kudu.source;

import com.dtstack.chunjun.config.FieldConfig;
import com.dtstack.chunjun.config.SyncConfig;
import com.dtstack.chunjun.connector.kudu.config.KuduSourceConfig;
import com.dtstack.chunjun.connector.kudu.converter.KuduRawTypeMapper;
import com.dtstack.chunjun.connector.kudu.converter.KuduSyncConverter;
import com.dtstack.chunjun.converter.RawTypeMapper;
import com.dtstack.chunjun.source.SourceFactory;
import com.dtstack.chunjun.util.JsonUtil;
import com.dtstack.chunjun.util.TableUtil;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

import java.util.ArrayList;
import java.util.List;

public class KuduSourceFactory extends SourceFactory {

    private final KuduSourceConfig sourceConf;

    public KuduSourceFactory(SyncConfig syncConfig, StreamExecutionEnvironment env) {
        super(syncConfig, env);

        sourceConf =
                JsonUtil.toObject(
                        JsonUtil.toJson(syncConfig.getReader().getParameter()),
                        KuduSourceConfig.class);
        sourceConf.setColumn(syncConfig.getReader().getFieldList());
        sourceConf.setKerberos(sourceConf.conventHadoopConfig());
        super.initCommonConf(sourceConf);
    }

    @Override
    public RawTypeMapper getRawTypeMapper() {
        return KuduRawTypeMapper::apply;
    }

    @Override
    public DataStream<RowData> createSource() {
        KuduInputFormatBuilder builder = new KuduInputFormatBuilder();

        builder.setKuduSourceConf(sourceConf);

        final RowType rowType = TableUtil.createRowType(sourceConf.getColumn(), getRawTypeMapper());

        List<FieldConfig> fieldConfList = sourceConf.getColumn();
        List<String> columnNameList = new ArrayList<>();
        fieldConfList.forEach(fieldConfig -> columnNameList.add(fieldConfig.getName()));

        builder.setRowConverter(
                new KuduSyncConverter(rowType, columnNameList), useAbstractBaseColumn);

        return createInput(builder.finish());
    }
}
