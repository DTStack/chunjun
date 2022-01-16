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

package com.dtstack.flinkx.connector.kudu.sink;

import com.dtstack.flinkx.conf.FieldConf;
import com.dtstack.flinkx.conf.SyncConf;
import com.dtstack.flinkx.connector.kudu.conf.KuduSinkConf;
import com.dtstack.flinkx.connector.kudu.converter.KuduColumnConverter;
import com.dtstack.flinkx.connector.kudu.converter.KuduRawTypeConverter;
import com.dtstack.flinkx.converter.RawTypeConverter;
import com.dtstack.flinkx.sink.SinkFactory;
import com.dtstack.flinkx.util.JsonUtil;
import com.dtstack.flinkx.util.TableUtil;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

import java.util.ArrayList;
import java.util.List;

/**
 * @author tiezhu
 * @since 2021/6/21 星期一
 */
public class KuduSinkFactory extends SinkFactory {

    private final KuduSinkConf sinkConf;

    public KuduSinkFactory(SyncConf syncConf) {
        super(syncConf);

        sinkConf =
                JsonUtil.toObject(
                        JsonUtil.toJson(syncConf.getWriter().getParameter()), KuduSinkConf.class);
        sinkConf.setColumn(syncConf.getWriter().getFieldList());
        sinkConf.setKerberos(sinkConf.conventHadoopConfig());
        super.initFlinkxCommonConf(sinkConf);
    }

    @Override
    public RawTypeConverter getRawTypeConverter() {
        return KuduRawTypeConverter::apply;
    }

    @Override
    public DataStreamSink<RowData> createSink(DataStream<RowData> dataSet) {
        KuduOutputFormatBuilder builder = new KuduOutputFormatBuilder();
        List<String> columnNames = new ArrayList<>();

        builder.setSinkConf(sinkConf);
        List<FieldConf> fieldConfList = sinkConf.getColumn();
        fieldConfList.forEach(field -> columnNames.add(field.getName()));

        final RowType rowType = TableUtil.createRowType(fieldConfList, getRawTypeConverter());
        builder.setRowConverter(new KuduColumnConverter(rowType, columnNames));
        return createOutput(dataSet, builder.finish());
    }
}
