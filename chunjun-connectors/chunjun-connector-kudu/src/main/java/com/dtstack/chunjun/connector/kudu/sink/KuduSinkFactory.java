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

package com.dtstack.chunjun.connector.kudu.sink;

import com.dtstack.chunjun.config.FieldConfig;
import com.dtstack.chunjun.config.SyncConfig;
import com.dtstack.chunjun.connector.kudu.config.KuduSinkConfig;
import com.dtstack.chunjun.connector.kudu.converter.KuduRawTypeMapper;
import com.dtstack.chunjun.connector.kudu.converter.KuduSyncConverter;
import com.dtstack.chunjun.converter.RawTypeMapper;
import com.dtstack.chunjun.sink.SinkFactory;
import com.dtstack.chunjun.util.JsonUtil;
import com.dtstack.chunjun.util.TableUtil;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

import java.util.ArrayList;
import java.util.List;

public class KuduSinkFactory extends SinkFactory {

    private final KuduSinkConfig sinkConfig;

    public KuduSinkFactory(SyncConfig syncConfig) {
        super(syncConfig);

        sinkConfig =
                JsonUtil.toObject(
                        JsonUtil.toJson(syncConfig.getWriter().getParameter()),
                        KuduSinkConfig.class);
        sinkConfig.setColumn(syncConfig.getWriter().getFieldList());
        sinkConfig.setKerberos(sinkConfig.conventHadoopConfig());
        super.initCommonConf(sinkConfig);
    }

    @Override
    public RawTypeMapper getRawTypeMapper() {
        return KuduRawTypeMapper::apply;
    }

    @Override
    public DataStreamSink<RowData> createSink(DataStream<RowData> dataSet) {
        KuduOutputFormatBuilder builder = new KuduOutputFormatBuilder();
        List<String> columnNames = new ArrayList<>();

        builder.setSinkConfig(sinkConfig);
        List<FieldConfig> fieldConfList = sinkConfig.getColumn();
        fieldConfList.forEach(field -> columnNames.add(field.getName()));

        final RowType rowType = TableUtil.createRowType(fieldConfList, getRawTypeMapper());
        builder.setRowConverter(new KuduSyncConverter(rowType, columnNames), useAbstractBaseColumn);
        return createOutput(dataSet, builder.finish());
    }
}
