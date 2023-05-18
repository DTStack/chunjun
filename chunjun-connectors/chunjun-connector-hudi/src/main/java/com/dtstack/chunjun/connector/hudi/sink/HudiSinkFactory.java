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
package com.dtstack.chunjun.connector.hudi.sink;

import com.dtstack.chunjun.config.FieldConfig;
import com.dtstack.chunjun.config.SyncConfig;
import com.dtstack.chunjun.connector.hudi.adaptor.HudiOnChunjunAdaptor;
import com.dtstack.chunjun.connector.hudi.converter.HudiRowDataMapping;
import com.dtstack.chunjun.converter.RawTypeMapper;
import com.dtstack.chunjun.sink.SinkFactory;
import com.dtstack.chunjun.util.TableUtil;

import org.apache.flink.api.java.typeutils.TypeExtractionException;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.data.RowData;

import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.configuration.FlinkOptions;

import java.util.List;
import java.util.Map;

import static org.apache.hudi.common.util.ValidationUtils.checkArgument;

/** chunjun's hudi async function implements the factory */
public class HudiSinkFactory extends SinkFactory {
    HudiOnChunjunAdaptor adaptor;

    public HudiSinkFactory(SyncConfig syncConf) {
        super(syncConf);
        Map<String, String> hudiConfig =
                (Map<String, String>) syncConf.getWriter().getParameter().get("hudiConfig");
        checkArgument(
                !StringUtils.isNullOrEmpty(hudiConfig.get(FlinkOptions.PATH.key())),
                "Option [path] should not be empty.");
        this.adaptor = new HudiOnChunjunAdaptor(syncConf, Configuration.fromMap(hudiConfig));
    }

    @Override
    public RawTypeMapper getRawTypeMapper() {
        return HudiRowDataMapping::apply;
    }

    @Override
    public DataStreamSink<RowData> createSink(DataStream<RowData> dataSet) {
        //        RowType rowType = null;
        List<FieldConfig> fieldList = syncConfig.getWriter().getFieldList();
        ResolvedSchema schema = TableUtil.createTableSchema(fieldList, getRawTypeMapper());

        try {
            return adaptor.createHudiSinkDataStream(dataSet, schema);
        } catch (TypeExtractionException e) {
            e.printStackTrace();
            System.exit(1);
        }
        return null;
    }
}
